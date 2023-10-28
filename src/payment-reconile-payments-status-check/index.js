const _ = require("lodash");
const moment = require("moment");
const axios = require("axios");
const {
    mysql, sendgridMail, paymentHelper, slack, config,
} = require("../../modules");

/* ************** This Cron is used to check status of failed | initiated | reconcile payments And report, resolve Discrepancies************** */

async function getFailedPaymentsListByTime(from, to, minCreatedAt) {
    const sql = `SELECT
                    pi.*,
                    s.mobile
                FROM
                    payment_info pi
                    JOIN students s on s.student_id = pi.student_id
                WHERE
                    pi.updated_at >= ?
                    AND pi.updated_at <= ?
                    AND pi.created_at >= ?
                ORDER BY
                    pi.id DESC`;
    console.log(sql);
    console.log(from);
    console.log(to);
    console.log(minCreatedAt);

    return mysql.pool.query(sql, [from, to, minCreatedAt]).then((res) => res[0]);
}

async function razorPayCheckWithStatus(razorPayResponse, paymentInfoStatus) {
    let response = false;
    if (!_.isEmpty(razorPayResponse) && razorPayResponse.count > 0) {
        for (let i = 0; i < razorPayResponse.count; i++) {
            if ((razorPayResponse.items[i].status == "captured") && (paymentInfoStatus == "FAILURE" || paymentInfoStatus == "INITIATED" || paymentInfoStatus == "RECONCILE")) {
                response = true;
                return response;
            }
            if ((razorPayResponse.items[i].status == "failed") && (paymentInfoStatus == "RECONCILE")) {
                response = true;
                return response;
            }
        }
    }
    return response;
}

async function reconcilePIEntriesWithRzp(paymentInfoPayments) {
    const flaggedPayments = [];
    try {
        let i = 0;
        console.log("paymentInfoPaymentslength", paymentInfoPayments.length);
        _.remove(paymentInfoPayments, (n) => (n.source.toLowerCase() != "razorpay" || n.partner_order_id == null || !(n.partner_order_id.split("_")[0].includes("order"))));
        console.log("paymentInfoPaymentslength", paymentInfoPayments.length);
        while (i < paymentInfoPayments.length) {
            const iterationLevel = i + 25;
            const promises = [];
            for (; i < paymentInfoPayments.length && i <= iterationLevel; i++) {
                promises.push(paymentHelper.fetchPaymentsByOrderId(paymentInfoPayments[i].partner_order_id));
            }
            // eslint-disable-next-line no-await-in-loop
            const response = await Promise.all(promises);
            console.log(response);
            const rzpStatusPromises = [];
            for (let j = 0; j < response.length; j++) {
                rzpStatusPromises.push(razorPayCheckWithStatus(response[j], paymentInfoPayments[iterationLevel - 25 + j].status));
            }
            // eslint-disable-next-line no-await-in-loop
            const statusResponses = await Promise.all(rzpStatusPromises);
            console.log(statusResponses);
            for (let j = 0; j < response.length; j++) {
                if (statusResponses[j]) {
                    const finalDatum = {};
                    finalDatum.partner_order_id = paymentInfoPayments[iterationLevel - 25 + j].partner_order_id;
                    finalDatum.source = paymentInfoPayments[iterationLevel - 25 + j].source;
                    finalDatum.student_id = paymentInfoPayments[iterationLevel - 25 + j].student_id;
                    finalDatum.mobile = paymentInfoPayments[iterationLevel - 25 + j].mobile;
                    finalDatum.total_amount = paymentInfoPayments[iterationLevel - 25 + j].total_amount;
                    finalDatum.amount = paymentInfoPayments[iterationLevel - 25 + j].amount;
                    finalDatum.wallet_amount = paymentInfoPayments[iterationLevel - 25 + j].wallet_amount;
                    flaggedPayments.push(finalDatum);
                }
            }
        }
        console.log("pi rzp done");
        return flaggedPayments;
    } catch (e) {
        console.log("e2", e);
        throw new Error(JSON.stringify(e));
    }
}

async function resolveDiscrepancies(list) {
    try {
        for (let i = 0; i < list.length; i++) {
            console.log(list[i]);
            const options = {
                url: `https://api.doubtnut.com/v1/payment/recon-payment-entries-by-order?partner_order_id=${list[i].partner_order_id}&source=${list[i].source}`,
                method: "GET",
                headers: {
                    "Content-Type": "application/json",
                },
            };
            console.log(options);
            // eslint-disable-next-line no-await-in-loop
            const { data } = await axios(options);
            console.log(data);
        }
    } catch (e) {
        console.log(e);
        return e;
    }
}

async function start(job) {
    const fromEmail = "autobot@doubtnut.com";
    const toEmail = "prashant.gupta@doubtnut.com";
    const ccList = ["dipankar@doubtnut.com"];
    const blockNew = [];
    try {
        const paymentEntriesFromDate = moment().add(5, "h").add(30, "minutes").subtract(7, "d")
            .startOf("day");
        const paymentEntriesToDate = moment().add(5, "h").add(30, "minutes").subtract(10, "minutes");
        const minCreatedAt = moment().add(5, "h").add(30, "minutes").subtract(16, "d")
            .startOf("day");

        const failedPaymentsList = await getFailedPaymentsListByTime(paymentEntriesFromDate.format("YYYY-MM-DD HH:mm:ss"), paymentEntriesToDate.format("YYYY-MM-DD HH:mm:ss"), minCreatedAt.format("YYYY-MM-DD HH:mm:ss"));
        console.log("final_array", failedPaymentsList.length);

        const discrepancyList = await reconcilePIEntriesWithRzp(failedPaymentsList);

        console.log(discrepancyList);
        await resolveDiscrepancies(discrepancyList);
        try {
            if (discrepancyList.length) {
                await sendgridMail.sendMail(fromEmail, toEmail, "CRON | Payment Reconcile Payments Status Reconciliation cron ran successfully", `Daily Report - Payment Status Reconciliation,\n Discrepancies: \n${JSON.stringify(discrepancyList)}`, [], ccList);
                blockNew.push({
                    type: "section",
                    text: { type: "mrkdwn", text: `CRON | Payment Reconcile Payments Status Reconciliation cron ran successfully:\n*Payment Status Reconciliation Daily Discrepancies:*\n\`\`\`${JSON.stringify(discrepancyList.map((obj) => obj))}\`\`\`` },
                });
                await slack.sendMessage("#payments-team-dev", blockNew, config.paymentsAutobotSlackAuth);
            }
        } catch (e) {
            console.log(e);
            await sendgridMail.sendMail(fromEmail, toEmail, "CRON | ALERT!!! Exception in Payment Reconcile Payments Status Reconciliation cron ", JSON.stringify(e), [], ccList);
            blockNew.push({
                type: "section",
                text: { type: "mrkdwn", text: `CRON | ALERT!!! Exception in Payment Reconcile Daily Payments Status Reconciliation cron <@U01MJU54A21> <@U0273ABLEPL> <@ULGN432HL>:\n\`\`\`${e.stack}\`\`\`` },
            });
            await slack.sendMessage("#payments-team", blockNew, config.paymentsAutobotSlackAuth);
        }
        return { err: null, data: null };
    } catch (e) {
        console.log("e1", e);
        await sendgridMail.sendMail(fromEmail, toEmail, "CRON | ALERT!!! Exception in Payment Reconcile Payments Status Reconciliation cron ", JSON.stringify(e), [], ccList);
        blockNew.push({
            type: "section",
            text: { type: "mrkdwn", text: `CRON | ALERT!!! Exception in Payment Reconcile Daily Payments Status Reconciliation cron <@U01MJU54A21> <@U0273ABLEPL> <@ULGN432HL>:\n\`\`\`${e.stack}\`\`\`` },
        });
        await slack.sendMessage("#payments-team", blockNew, config.paymentsAutobotSlackAuth);
        return { e };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "11 00 * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
