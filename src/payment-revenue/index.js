const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const moment = require("moment");

const {
    mysql, sendgridMail, slack, config,
} = require("../../modules");

async function getSuccessfulPaymentsListByTime(from, to, minCreatedAt) {
    const sql = `SELECT
                    pi.student_id,
                    pi.created_at,
                    pi.updated_at,
                    pi.order_id,
                    pi.partner_txn_id,
                    pi.source,
                    pi.mode,
                    pi.payment_for,
                    pi.coupon_code,
                    pi.total_amount,
                    pi.amount,
                    pi.discount,
                    pi.wallet_amount,
                    wt.cash_amount as wallet_cash_amount,
                    wt.reward_amount as wallet_reward_amount,
                    sps.end_date,
                    p.name,
                    s.mobile
                FROM
                    payment_info AS pi
                    LEFT JOIN wallet_transaction AS wt on wt.payment_info_id = pi.id
                    LEFT JOIN students AS s on s.student_id = pi.student_id
                    LEFT JOIN student_package_subscription AS sps on sps.payment_info_id = pi.id
                    LEFT JOIN variants v on v.id = pi.variant_id
                    LEFT JOIN package p on p.id = v.package_id
                WHERE
                    pi.updated_at > ?
                    AND pi.updated_at < ?
                    AND pi.created_at > ?
                    AND pi.status = 'SUCCESS'
                ORDER BY
                    pi.id DESC`;
    // console.log(sql);
    return mysql.pool.query(sql, [from, to, minCreatedAt]).then((res) => res[0]);
}

async function generateCSVFile(finalpayments, reportName) {
    try {
        const headerKeys = Object.keys(finalpayments[0]);
        const header = [];
        for (let i = 0; i < headerKeys.length; i++) {
            header.push({
                id: headerKeys[i],
                title: headerKeys[i],
            });
        }
        const csvWriter = createCsvWriter({
            path: `${reportName}.csv`,
            header,
        });
        await csvWriter.writeRecords(finalpayments);
    } catch (e) {
        console.log("csv error", e);
        return e;
    }
}

async function start(job) {
    const fromEmail = "autobot@doubtnut.com";
    const toEmail = "prashant.gupta@doubtnut.com";
    const ccList = ["gauravm@doubtnut.com", "dipankar@doubtnut.com", "aakash.dwivedi@doubtnut.com"];
    const blockNew = [];
    try {
        const paymentEntriesFromDate = moment().add(5, "h").add(30, "minutes").subtract(1, "d")
            .startOf("day");
        const paymentEntriesToDate = moment().add(5, "h").add(30, "minutes").subtract(1, "d")
            .endOf("day");
        const minCreatedAt = moment().add(5, "h").add(30, "minutes").subtract(16, "d")
            .startOf("day");

        const successfulPayments = await getSuccessfulPaymentsListByTime(paymentEntriesFromDate.format("YYYY-MM-DD HH:mm:ss"), paymentEntriesToDate.format("YYYY-MM-DD HH:mm:ss"), minCreatedAt.format("YYYY-MM-DD HH:mm:ss"));
        console.log("final_array", successfulPayments.length);

        const finalArray = [];
        for (let i = 0; i < successfulPayments.length; i++) {
            const finalDatum = {};
            finalDatum.student_id = successfulPayments[i].student_id;
            finalDatum.mobile = successfulPayments[i].mobile;
            finalDatum.date = moment(successfulPayments[i].updated_at).format("YYYY-MM-DD HH:mm:ss");
            console.log("id: 1", successfulPayments[i].order_id);
            finalDatum.course_name = successfulPayments[i].name;
            finalDatum.course_end_date = successfulPayments[i].end_date;
            finalDatum.order_id = ` ${successfulPayments[i].order_id} `;
            console.log("id: 2", finalDatum.order_id);
            finalDatum.partner_txn_id = ` ${successfulPayments[i].partner_txn_id} `;
            finalDatum.source = successfulPayments[i].source;
            finalDatum.mode = successfulPayments[i].mode;
            finalDatum.payment_for = successfulPayments[i].payment_for;
            finalDatum.coupon_code = successfulPayments[i].coupon_code;
            finalDatum.total_amount = successfulPayments[i].total_amount;
            finalDatum.discount = successfulPayments[i].discount;
            finalDatum.amount = successfulPayments[i].amount;
            finalDatum.wallet_cash_amount = successfulPayments[i].wallet_cash_amount;
            finalDatum.wallet_reward_amount = successfulPayments[i].wallet_reward_amount;
            finalDatum.total_revenue_from_amount = null;
            finalArray.push(finalDatum);
        }

        try {
            const date = moment().add(5, "h").add(30, "minutes").subtract(1, "d")
                .format("YYYY-MM-DD");
            if (finalArray.length) {
                await generateCSVFile(finalArray, `SuccessfulPaymentsRevenue_${date}`);
                const filesList = [`SuccessfulPaymentsRevenue_${date}.csv`];
                await sendgridMail.sendMail(fromEmail, toEmail, "CRON | Payment Revenue cron ran successfully", "Daily Report - Payment Revenue", filesList, ccList);
            } else {
                await sendgridMail.sendMail(fromEmail, toEmail, "CRON | Payment Revenue cron ran successfully", "No Successful Payment found", [], ccList);
            }
        } catch (e) {
            console.log(e);
            await sendgridMail.sendMail(fromEmail, toEmail, "CRON | ALERT!!! Exception in Payment Revenue cron ", JSON.stringify(e), [], ccList);
            blockNew.push({
                type: "section",
                text: { type: "mrkdwn", text: `CRON | ALERT!!! Exception in Payment Revenue cron <@U01MJU54A21> <@U0273ABLEPL> <@ULGN432HL>:\n\`\`\`${e.stack}\`\`\`` },
            });
            await slack.sendMessage("#payments-team", blockNew, config.paymentsAutobotSlackAuth);
        }
        return { err: null, data: null };
    } catch (e) {
        console.log("e1", e);
        await sendgridMail.sendMail(fromEmail, toEmail, "CRON | ALERT!!! Exception in Payment Revenue cron ", JSON.stringify(e), [], ccList);
        blockNew.push({
            type: "section",
            text: { type: "mrkdwn", text: `CRON | ALERT!!! Exception in Payment Revenue cron <@U01MJU54A21> <@U0273ABLEPL> <@ULGN432HL>:\n\`\`\`${e.stack}\`\`\`` },
        });
        await slack.sendMessage("#payments-team", blockNew, config.paymentsAutobotSlackAuth);
        return { e };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "07 12 * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
