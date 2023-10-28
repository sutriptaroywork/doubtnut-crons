/* eslint-disable no-await-in-loop */
const fastcsv = require("fast-csv");
const fs = require("fs");
const moment = require("moment");
const rp = require("request-promise");

const {
    mysql, sendgridMail, slack, config,
} = require("../../modules");

async function getSuccessfulPaymentsListByTime(from, to, minCreatedAt) {
    const sql = `SELECT
                    pi.id AS pi_id,
                    pi.status,
                    pi.partner_txn_id,
                    pi.student_id,
                    pi.amount,
                    pi.wallet_amount,
                    pi.total_amount,
                    s.mobile,
                    sps.id AS sps_id,
                    sps.start_date,
                    sps.end_date,
                    sps.is_active AS sps_is_active,
                    ps.id AS ps_id,
                    ps.subscription_id,
                    ps.txn_id,
                    ps.amount_paid AS ps_amount,
                    wt.id as wallet_transaction_id,
                    wt.cash_amount AS wt_cash_amount,
                    wt.reward_amount AS wt_reward_amount,
                    pim.id AS meta_id,
                    pim.wallet_cash_amount AS meta_wallet_cash_amount,
                    pim.wallet_reward_amount AS meta_wallet_reward_amount
                FROM
                    payment_info AS pi
                    JOIN students AS s ON s.student_id = pi.student_id
                    LEFT JOIN payment_summary AS ps ON ps.txn_id = pi.partner_txn_id
                    LEFT JOIN student_package_subscription AS sps ON pi.id = sps.payment_info_id
                    LEFT JOIN wallet_transaction AS wt on pi.id = wt.payment_info_id
                    LEFT JOIN payment_info_meta AS pim on pi.id = pim.payment_info_id
                WHERE
                    pi.updated_at >= ?
                    AND pi.updated_at <= ?
                    AND pi.created_at > ?
                    AND pi.status in ('SUCCESS', 'RECONCILE')
                    and pi.payment_for in ("course_package", 'vip_offline')
                ORDER BY
                    pi.id DESC`;
    // console.log(sql);
    return mysql.pool.query(sql, [from, to, minCreatedAt]).then((res) => res[0]);
}

async function getPaymentSummaryBySubscriptionId(sps_id) {
    const sql = `SELECT
                    id AS ps_id,
                    subscription_id,
                    txn_id,
                    amount_paid AS ps_amount
                FROM
                    payment_summary
                WHERE
                    subscription_id = ?
                ORDER BY
                    id DESC`;
    // console.log(sql);
    return mysql.pool.query(sql, [sps_id]).then((res) => res[0]);
}

async function getSubscriptionEntryById(id) {
    const sql = `SELECT
                    id AS sps_id,
                    start_date,
                    end_date,
                    is_active AS sps_is_active
                FROM
                    student_package_subscription
                WHERE
                    id = ?
                ORDER BY
                    id DESC`;
    // console.log(sql);
    return mysql.pool.query(sql, [id]).then((res) => res[0]);
}

async function getSuccessfulWalletPaymentsListByTime(from, to, minCreatedAt) {
    const sql = `SELECT
                    pi.id AS pi_id,
                    pi.status,
                    pi.student_id,
                    pi.amount,
                    pi.total_amount,
                    s.mobile,
                    pi.partner_txn_id,
                    wt.id AS wt_id
                FROM
                    payment_info AS pi
                    JOIN students AS s ON s.student_id = pi.student_id
                    LEFT JOIN wallet_transaction AS wt ON pi.id = wt.payment_info_id
                WHERE
                    pi.updated_at >= ?
                    AND pi.updated_at <= ?
                    AND pi.created_at > ?
                    AND pi.status = 'SUCCESS'
                    AND pi.payment_for = 'wallet'
                    AND wt.id is null
                ORDER BY
                    pi.id DESC`;
    return mysql.pool.query(sql, [from, to, minCreatedAt]).then((res) => res[0]);
}

async function reconcileSuccessfulPaymentsInPSANDSPS(successfulPayments) {
    const discrepancies = {
        missingPayments: [],
        amountDiscrepancies: [],
    };
    try {
        for (let i = 0; i < successfulPayments.length; i++) {
            let missingPsEntry = false;
            if (successfulPayments[i].sps_id == null && successfulPayments[i].ps_id == null) {
                const finalDatum = {};
                finalDatum.pi_id = successfulPayments[i].pi_id;
                finalDatum.sps_id = null;
                finalDatum.ps_id = null;
                finalDatum.error = "Missing Entry in ps and sps";
                finalDatum.student_id = successfulPayments[i].student_id;
                finalDatum.mobile = successfulPayments[i].mobile;
                finalDatum.amount = successfulPayments[i].amount;
                finalDatum.total_amount = successfulPayments[i].total_amount;
                finalDatum.wallet_amount = successfulPayments[i].wallet_amount;
                discrepancies.missingPayments.push(finalDatum);
                missingPsEntry = true;
            } else if (successfulPayments[i].sps_id == null) {
                const finalDatum = {};
                // eslint-disable-next-line no-await-in-loop
                const spsEntry = await getSubscriptionEntryById(successfulPayments[i].subscription_id);
                if (spsEntry.length === 0) {
                    finalDatum.pi_id = successfulPayments[i].pi_id;
                    finalDatum.sps_id = null;
                    finalDatum.ps_id = successfulPayments[i].ps_id;
                    finalDatum.error = "Missing Entry in sps";
                    finalDatum.student_id = successfulPayments[i].student_id;
                    finalDatum.mobile = successfulPayments[i].mobile;
                    finalDatum.amount = successfulPayments[i].amount;
                    finalDatum.total_amount = successfulPayments[i].total_amount;
                    finalDatum.wallet_amount = successfulPayments[i].wallet_amount;
                    discrepancies.missingPayments.push(finalDatum);
                }
            } else if (successfulPayments[i].ps_id == null) {
                const finalDatum = {};
                // eslint-disable-next-line no-await-in-loop
                const psEntry = await getPaymentSummaryBySubscriptionId(successfulPayments[i].sps_id);
                finalDatum.pi_id = successfulPayments[i].pi_id;
                finalDatum.sps_id = successfulPayments[i].sps_id;
                finalDatum.ps_id = psEntry.length ? psEntry[0].ps_id : null;
                finalDatum.error = psEntry.length ? "Missing txn_id in ps" : "Missing Entry in ps";
                finalDatum.student_id = successfulPayments[i].student_id;
                finalDatum.mobile = successfulPayments[i].mobile;
                finalDatum.amount = successfulPayments[i].amount;
                finalDatum.total_amount = successfulPayments[i].total_amount;
                finalDatum.wallet_amount = successfulPayments[i].wallet_amount;
                discrepancies.missingPayments.push(finalDatum);
                if (psEntry.length) {
                    successfulPayments[i].ps_id = psEntry[0].ps_id;
                    successfulPayments[i].subscription_id = psEntry[0].subscription_id;
                    successfulPayments[i].txn_id = psEntry[0].txn_id;
                    successfulPayments[i].ps_amount = psEntry[0].ps_amount;
                }
                missingPsEntry = !psEntry.length;
            }
            if (parseInt(successfulPayments[i].wallet_amount) != 0) {
                if (successfulPayments[i].wallet_transaction_id == null) {
                    const finalDatum = {};
                    finalDatum.pi_id = successfulPayments[i].pi_id;
                    finalDatum.sps_id = successfulPayments[i].sps_id;
                    finalDatum.ps_id = successfulPayments[i].ps_id;
                    finalDatum.error = "Missing Entry in wallet transaction";
                    finalDatum.student_id = successfulPayments[i].student_id;
                    finalDatum.mobile = successfulPayments[i].mobile;
                    finalDatum.amount = successfulPayments[i].amount;
                    finalDatum.total_amount = successfulPayments[i].total_amount;
                    finalDatum.wallet_amount = successfulPayments[i].wallet_amount;
                    discrepancies.amountDiscrepancies.push(finalDatum);
                } else if (successfulPayments[i].meta_id != null && parseInt(successfulPayments[i].meta_wallet_cash_amount) != parseInt(successfulPayments[i].wt_cash_amount)) {
                    console.log("meta entry");
                    const finalDatum = {};
                    finalDatum.pi_id = successfulPayments[i].pi_id;
                    finalDatum.sps_id = successfulPayments[i].sps_id;
                    finalDatum.ps_id = successfulPayments[i].ps_id;
                    finalDatum.error = "Wrong Cash Amount Entry in wallet transaction";
                    finalDatum.student_id = successfulPayments[i].student_id;
                    finalDatum.mobile = successfulPayments[i].mobile;
                    finalDatum.amount = successfulPayments[i].amount;
                    finalDatum.total_amount = successfulPayments[i].total_amount;
                    finalDatum.wallet_amount = successfulPayments[i].wallet_amount;
                    discrepancies.amountDiscrepancies.push(finalDatum);
                } else if (successfulPayments[i].meta_id != null && parseInt(successfulPayments[i].meta_wallet_reward_amount) != parseInt(successfulPayments[i].wt_reward_amount)) {
                    const finalDatum = {};
                    finalDatum.pi_id = successfulPayments[i].pi_id;
                    finalDatum.sps_id = successfulPayments[i].sps_id;
                    finalDatum.ps_id = successfulPayments[i].ps_id;
                    finalDatum.error = "Wrong Reward Amount Entry in wallet transaction";
                    finalDatum.student_id = successfulPayments[i].student_id;
                    finalDatum.mobile = successfulPayments[i].mobile;
                    finalDatum.amount = successfulPayments[i].amount;
                    finalDatum.total_amount = successfulPayments[i].total_amount;
                    finalDatum.wallet_amount = successfulPayments[i].wallet_amount;
                    discrepancies.amountDiscrepancies.push(finalDatum);
                } else if (parseInt(successfulPayments[i].wallet_amount) != (parseInt(successfulPayments[i].wt_cash_amount) + parseInt(successfulPayments[i].wt_reward_amount))) {
                    const finalDatum = {};
                    finalDatum.pi_id = successfulPayments[i].pi_id;
                    finalDatum.sps_id = successfulPayments[i].sps_id;
                    finalDatum.ps_id = successfulPayments[i].ps_id;
                    finalDatum.error = "Wrong Total Wallet(Cash + Reward) Amount Entry in wallet transaction";
                    finalDatum.student_id = successfulPayments[i].student_id;
                    finalDatum.mobile = successfulPayments[i].mobile;
                    finalDatum.amount = successfulPayments[i].amount;
                    finalDatum.total_amount = successfulPayments[i].total_amount;
                    finalDatum.wallet_amount = successfulPayments[i].wallet_amount;
                    discrepancies.amountDiscrepancies.push(finalDatum);
                } else if (!missingPsEntry && parseInt(successfulPayments[i].ps_amount) != (parseInt(successfulPayments[i].wt_cash_amount) + parseInt(successfulPayments[i].amount))) {
                    const finalDatum = {};
                    finalDatum.pi_id = successfulPayments[i].pi_id;
                    finalDatum.sps_id = successfulPayments[i].sps_id;
                    finalDatum.ps_id = successfulPayments[i].ps_id;
                    finalDatum.error = "Wrong Amount Paid Entry in payment summary";
                    finalDatum.student_id = successfulPayments[i].student_id;
                    finalDatum.mobile = successfulPayments[i].mobile;
                    finalDatum.amount = successfulPayments[i].amount;
                    finalDatum.total_amount = successfulPayments[i].total_amount;
                    finalDatum.wallet_amount = successfulPayments[i].wallet_amount;
                    discrepancies.amountDiscrepancies.push(finalDatum);
                }
            }
        }
        return discrepancies;
    } catch (e) {
        console.log(e);
        throw new Error(JSON.stringify(e));
    }
}

async function reconcileSuccessfulPaymentsInWalletTransaction(successfulWalletPayments) {
    const missingPayments = [];
    try {
        for (let i = 0; i < successfulWalletPayments.length; i++) {
            const finalDatum = {};
            finalDatum.pi_id = successfulWalletPayments[i].pi_id;
            finalDatum.wt_id = null;
            finalDatum.error = "Missing Entry in wallet_transaction";
            finalDatum.student_id = successfulWalletPayments[i].student_id;
            finalDatum.mobile = successfulWalletPayments[i].mobile;
            finalDatum.amount = successfulWalletPayments[i].amount;
            finalDatum.total_amount = successfulWalletPayments[i].total_amount;
            missingPayments.push(finalDatum);
        }
        return missingPayments;
    } catch (e) {
        console.log(e);
        throw new Error(JSON.stringify(e));
    }
}

async function resolveDiscrepancies(list) {
    try {
        for (let i = 0; i < list.length; i++) {
            if (list[i].error.includes("Missing Entry")) {
                const options = {
                    url: `https://api.doubtnut.com/v1/payment/create-missing-payment-entries?payment_info_id=${list[i].pi_id}`,
                    method: "GET",
                };
                rp(options);
            }
        }
    } catch (e) {
        console.log(e);
        throw new Error(JSON.stringify(e));
    }
}

async function generateCSVFile(finalpayments, reportName) {
    try {
        const ws = fs.createWriteStream(`${reportName}.csv`);
        fastcsv
            .write(finalpayments, { headers: true })
            .pipe(ws);
    } catch (e) {
        console.log("csv error", e);
        throw new Error(JSON.stringify(e));
    }
}

async function start(job) {
    const fromEmail = "autobot@doubtnut.com";
    const toEmail = "prashant.gupta@doubtnut.com";
    const ccList = ["dipankar@doubtnut.com", "aditya@doubtnut.com", "prakher.gaushal@doubtnut.com"];
    const blockNew = [];
    // const ccList = ["divdetoantors@gmail.com"];
    try {
        const paymentEntriesFromDate = moment().add(5, "h").add(30, "minutes")
            .subtract(1, "h");
        const paymentEntriesToDate = moment().add(5, "h").add(30, "minutes")
            .subtract(15, "minutes");
        const minCreatedAt = moment().add(5, "h").add(30, "minutes").subtract(16, "d")
            .startOf("day");

        let i = 0;
        let internalDiscrepancies;
        let internalWalletDiscrepancies;
        do {
            const successfulPayments = await getSuccessfulPaymentsListByTime(paymentEntriesFromDate.format("YYYY-MM-DD HH:mm:ss"), paymentEntriesToDate.format("YYYY-MM-DD HH:mm:ss"), minCreatedAt.format("YYYY-MM-DD HH:mm:ss"));
            const successfulWalletPayments = await getSuccessfulWalletPaymentsListByTime(paymentEntriesFromDate.format("YYYY-MM-DD HH:mm:ss"), paymentEntriesToDate.format("YYYY-MM-DD HH:mm:ss"), minCreatedAt.format("YYYY-MM-DD HH:mm:ss"));
            console.log("final_internal_array", successfulPayments.length);
            console.log("final_wallet_array", successfulWalletPayments.length);
            // eslint-disable-next-line no-await-in-loop
            [internalDiscrepancies, internalWalletDiscrepancies] = await Promise.all([
                reconcileSuccessfulPaymentsInPSANDSPS(successfulPayments),
                reconcileSuccessfulPaymentsInWalletTransaction(successfulWalletPayments),
            ]);
            console.log("Internal Payment Discrepancies: ", internalDiscrepancies);
            console.log("Wallet Discrepancies: ", internalWalletDiscrepancies);

            if (i === 0) {
                // eslint-disable-next-line no-await-in-loop
                await Promise.all([
                    resolveDiscrepancies(internalDiscrepancies.missingPayments),
                    resolveDiscrepancies(internalWalletDiscrepancies),
                ]);
            }

            // eslint-disable-next-line no-await-in-loop
            await new Promise((resolve) => setTimeout(resolve, 5000));
        } while (i++ < 1);
        try {
            if (internalDiscrepancies.missingPayments.length || internalWalletDiscrepancies.length) {
                // await generateCSVFile(internalDiscrepancies, `InternalReconciliation_${date}`);
                await sendgridMail.sendMail(fromEmail, toEmail, "CRON | Internal Payment Reconciliation cron ran successfully", `Daily Report - Internal Payment Recociliation \n${JSON.stringify(internalDiscrepancies)} \nInternal Wallet Reconciliation \n${JSON.stringify(internalWalletDiscrepancies)}`, [], ccList);
                blockNew.push({
                    type: "section",
                    text: { type: "mrkdwn", text: `CRON | Internal Payment Reconciliation cron ran successfully:\n*Internal Payment Recociliation*\n\`\`\`${JSON.stringify(internalDiscrepancies.missingPayments.map((obj) => obj))}\`\`\`\n*Internal Wallet Reconciliation*\n\`\`\`${JSON.stringify(internalWalletDiscrepancies.map((obj) => obj))}\`\`\`` },
                });
                await slack.sendMessage("#payments-team-dev", blockNew, config.paymentsAutobotSlackAuth);
            }
            if (internalDiscrepancies.amountDiscrepancies.length) {
                blockNew.push({
                    type: "section",
                    text: { type: "mrkdwn", text: `CRON | Internal Payment Reconciliation cron ran successfully <@U01MJU54A21> <@U0273ABLEPL> <@ULGN432HL>:\n*Internal Payment Amount Recociliation*\n\`\`\`${JSON.stringify(internalDiscrepancies.amountDiscrepancies.map((obj) => obj))}\`\`\`` },
                });
                await slack.sendMessage("#payments-team-dev", blockNew, config.paymentsAutobotSlackAuth);
            }
            // else {
            //     await sendgridMail.sendMail(fromEmail, toEmail, "CRON | Internal Payment Reconciliation cron ran successfully", "No Discrepancy found", [], ccList);
            // }
        } catch (e) {
            console.log(e);
            await sendgridMail.sendMail(fromEmail, toEmail, "CRON | ALERT!!! Exception in Internal Payment Reconciliation ", JSON.stringify(e), [], ccList);
            blockNew.push({
                type: "section",
                text: { type: "mrkdwn", text: `CRON | ALERT!!! Exception in Internal Payment Reconciliation <@U01MJU54A21> <@U0273ABLEPL> <@ULGN432HL>:\n\`\`\`${e.stack}\`\`\`` },
            });
            await slack.sendMessage("#payments-team", blockNew, config.paymentsAutobotSlackAuth);
        }
        return { err: null, data: null };
    } catch (e) {
        console.log("e1", e);
        await sendgridMail.sendMail(fromEmail, toEmail, "CRON | ALERT!!! Exception in Internal Payment Reconciliation ", JSON.stringify(e), [], ccList);
        blockNew.push({
            type: "section",
            text: { type: "mrkdwn", text: `CRON | ALERT!!! Exception in Internal Payment Reconciliation <@U01MJU54A21> <@U0273ABLEPL> <@ULGN432HL>:\n\`\`\`${e.stack}\`\`\`` },
        });
        await slack.sendMessage("#payments-team", blockNew, config.paymentsAutobotSlackAuth);
        return { e };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "3-59/15 * * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
