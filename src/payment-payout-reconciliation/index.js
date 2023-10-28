const _ = require("lodash");
const axios = require("axios");
const moment = require("moment");
const {
    mysql, config, sendgridMail, slack,
} = require("../../modules");

const bufferObj = Buffer.from(`${config.RAZORPAY_KEY_ID}:${config.RAZORPAY_KEY_SECRET}`, "utf8");
const base64String = bufferObj.toString("base64");

async function getDisbursmentByRzpPayoutId(id) {
    const sql = "select * from student_referral_paytm_disbursement where payout_id = ?";
    mysql.pool.query(sql, [id]).then((res) => res[0]);
}

async function fetchRzppayout({
    from, to, count, skip,
}) {
    const options = {
        method: "get",
        url: "https://api.razorpay.com/v1/payout-links",
        params: {
            from,
            to,
            count,
            skip,
        },
        headers: {
            Authorization: `Basic ${base64String}`,
        },
    };
    const { data } = await axios(options);
    return data;
}

async function fetchRzpAllPayouts(from, to) {
    let rzpPayments = [];
    let rzpResponseLength = 0;
    let rzpResponseAll = [];
    const maxIteration = 5;
    try {
        do {
            const promiseRzp = [];
            // eslint-disable-next-line no-await-in-loop
            // console.log("rzpResponse",rzpResponse.length);
            for (let i = 0; i < maxIteration; i++) {
                const skip = 100 * i + rzpResponseLength;
                promiseRzp.push(fetchRzppayout({
                    from,
                    to,
                    count: 100,
                    skip,
                }));
            }

            // eslint-disable-next-line no-await-in-loop
            rzpResponseAll = await Promise.all(promiseRzp);

            console.log("rzpResponseAll", rzpResponseAll);
            for (let i = 0; i < rzpResponseAll.length; i++) {
                const rzpResponse = rzpResponseAll[i];
                rzpPayments = rzpPayments.concat(rzpResponse.items);
                rzpResponseLength += rzpResponse.count;
            }
            console.log(rzpResponseLength);
        } while (rzpResponseAll[maxIteration - 1].count > 0);
        return rzpPayments;
    } catch (e) {
        console.log("e4", e);
        throw new Error(JSON.stringify(e));
    }
}

async function reconcilePayoutLinkEntriesAndAmount(rzpPayouts) {
    try {
        const missingPayments = [];

        const iterationLevel = 100;
        const rzpClone = JSON.parse(JSON.stringify(rzpPayouts));
        let batch = rzpClone.splice(0, iterationLevel);

        const disbursmentEntries = [];
        while (batch.length) {
            const promises = [];
            for (let i = 0; i < batch.length; i++) {
                console.log(batch[i].id);
                promises.push(getDisbursmentByRzpPayoutId(batch[i].id));
            }
            batch = rzpClone.splice(0, iterationLevel);
            // const paymentInfoResponses = await Promise.all(promises);
            // eslint-disable-next-line no-await-in-loop
            const ss = await Promise.all(promises);
            console.log(ss);

            for (let i = 0; i < ss.length; i++) {
                if (!_.isEmpty(ss[i])) {
                    disbursmentEntries.push(ss[i][0]);
                } else {
                    disbursmentEntries.push("");
                }
            }
        }

        for (let i = 0; i < rzpPayouts.length; i++) {
            if (!_.isEmpty(disbursmentEntries[i]) && !_.isEmpty(rzpPayouts[i])) {
                console.log("entry exist in disursment");
                if ((rzpPayouts[i].amount / 100) == disbursmentEntries[i].amount);
                else {
                    const finalDatum = {};
                    finalDatum.payount_amount = (rzpPayouts[i].amount / 100);
                    finalDatum.disbursment_amount = disbursmentEntries[i].amount;
                    finalDatum.disbusment_id = disbursmentEntries[i].id;
                    finalDatum.payout_id = rzpPayouts[i].id;
                    finalDatum.payout_status = rzpPayouts[i].status;
                    finalDatum.error = "Amount Missmatch";
                    missingPayments.push(finalDatum);
                }
            } else {
                const finalDatum = {};
                finalDatum.payount_amount = (rzpPayouts[i].amount / 100);
                finalDatum.disbursment_amount = null;
                finalDatum.disbusment_id = null;
                finalDatum.payout_id = rzpPayouts[i].id;
                finalDatum.payout_status = rzpPayouts[i].status;
                finalDatum.error = "No payout Entry in disbursment";
                missingPayments.push(finalDatum);
            }
        }
        return missingPayments;
    } catch (e) {
        console.log("e3", e);
        throw new Error(JSON.stringify(e));
    }
}
async function start(job) {
    const from = moment().add(5, "h").add(30, "minutes").subtract(2, "h");
    const to = moment().add(5, "h").add(30, "minutes");
    const fromEmail = "autobot@doubtnut.com";
    const toEmail = "prashant.gupta@doubtnut.com";
    const ccList = ["dipankar@doubtnut.com", "prakher.gaushal@doubtnut.com"];
    const blockNew = [];
    try {
        const allRzpPayoutLinks = await fetchRzpAllPayouts(from.unix(), to.unix());
        const missingPayouts = await reconcilePayoutLinkEntriesAndAmount(allRzpPayoutLinks);
        if (missingPayouts.length) {
            console.log(missingPayouts);
            await sendgridMail.sendMail(fromEmail, toEmail, "CRON | Payment Reconcile Payout Payments cron ran successfully", `Daily Report - Payment Payout Reconciliation,\n Discrepancies: \n${JSON.stringify(missingPayouts)}`, [], ccList);
            blockNew.push({
                type: "section",
                text: { type: "mrkdwn", text: "CRON | Payment Payout Reconciliation cron ran successfully <@U01MJU54A21> <@U0273ABLEPL> <@ULGN432HL>:\n*Payment Payout Reconciliation Discrepancies:*\n" },
            });

            for (let i = 0; i < missingPayouts.length; i++) {
                const dummy = missingPayouts.slice(0, 5);
                blockNew.push({
                    type: "section",
                    text: { type: "mrkdwn", text: `\`\`\`${JSON.stringify(dummy.map((obj) => obj))}\`\`\`\n` },
                });
            }
            console.log(await slack.sendMessage("#payments-team", blockNew, config.paymentsAutobotSlackAuth));
        }
    } catch (e) {
        console.log("e1", e);
        await sendgridMail.sendMail(fromEmail, toEmail, "CRON | ALERT!!! Exception in Payment Payout Reconciliation ", JSON.stringify(e), [], ccList);
        blockNew.push({
            type: "section",
            text: { type: "mrkdwn", text: `CRON | ALERT!!! Exception in Payment Payout Reconciliation <@U01MJU54A21> <@U0273ABLEPL> <@ULGN432HL>:\n\`\`\`${e.stack}\`\`\`` },
        });
        await slack.sendMessage("#payments-team", blockNew, config.paymentsAutobotSlackAuth);
        return { e };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "35 */1 * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
