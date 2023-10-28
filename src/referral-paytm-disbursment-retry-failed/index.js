/**
 * For all the pending/failed(is_paytm_disbursed = 2) entries before D-1 in table student_referral_paytm_disbursement
 * This CRON updates the order Id and set is_paytm_disbursed as null
 * so that referral-paytm-disbursment CRON ke retry the disbursment for these entries.
 * Reason: Once the payment get's failed for any reason, PAYTM doesn't retry for same orderId Phone no combination.
 */
const moment = require("moment");
const _ = require("lodash");
const { config, mysql, slack } = require("../../modules");

async function updateDisbursmentEntry(id) {
    const orderId = (moment(new Date()).format("YYYYMMDDHHmmssSSS")).toString() + Math.floor(Math.random() * 100);
    const sql = "UPDATE student_referral_paytm_disbursement set is_paytm_disbursed = null, order_id = ? where id = ?";
    return mysql.writePool.query(sql, [orderId, id]).then((res) => res[0]);
}

async function getAllPendingDisbursmentsFromPreviousDay() {
    const sql = "SELECT * from student_referral_paytm_disbursement where is_paytm_disbursed = 2 and amount > 150 and created_at <= DATE_SUB(NOW(), INTERVAL 1 DAY)";
    return mysql.pool.query(sql).then((res) => res[0]);
}

const paytmResponseCodesToSkip = [
    "DE_001",
    "DE_688",
    "DE_687",
    "DE_101",
    "DE_102",
    "DE_601",
    "DE_604",
    "DE_976",
    "DE_038",
];

const paytmResponseStatusToSkip = [
    "SUCCESS",
    "PENDING",
];

async function start(job) {
    try {
        const pendingDisbursments = await getAllPendingDisbursmentsFromPreviousDay();
        console.log(pendingDisbursments);
        for (let i = 0; i < pendingDisbursments.length; i++) {
            try {
                const paytmResponseArray = pendingDisbursments[i].paytm_response.split(/,|}|{/);
                console.log(`{${`${paytmResponseArray[1]},${paytmResponseArray[2]},${paytmResponseArray[3]}`}}`);
                const paytmResponse = JSON.parse(`{${paytmResponseArray[1]},${paytmResponseArray[2]},${paytmResponseArray[3]}}`);
                console.log(paytmResponse);

                if (!(_.includes(paytmResponseCodesToSkip, paytmResponse.statusCode) || _.includes(paytmResponseStatusToSkip, paytmResponse.status))) {
                    console.log("Pending Disbursment id", pendingDisbursments[i].id);
                    // eslint-disable-next-line no-await-in-loop
                    await updateDisbursmentEntry(pendingDisbursments[i].id);
                }
            } catch (e) {
                console.log(e);
                const blockNew = [{
                    type: "section",
                    text: { type: "mrkdwn", text: `CRON | ALERT!!! Exception in referral disursment retry for id: ${pendingDisbursments[i].id} \n\`\`\`${e.stack}\`\`\`` },
                }];
                // eslint-disable-next-line no-await-in-loop
                await slack.sendMessage("#payments-team-dev", blockNew, config.paymentsAutobotSlackAuth);
            }
        }
        return { err: null, data: null };
    } catch (e) {
        console.log(e);
        const blockNew = [{
            type: "section",
            text: { type: "mrkdwn", text: `CRON | ALERT!!! Exception in referral disursment retry <@U01MJU54A21> <@U0273ABLEPL> <@ULGN432HL>:\n\`\`\`${e.stack}\`\`\`` },
        }];
        await slack.sendMessage("#payments-team", blockNew, config.paymentsAutobotSlackAuth);
        return { e };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "07 14 * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
