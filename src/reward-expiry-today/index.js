const _ = require("lodash");
const moment = require("moment");
const rp = require("request-promise");
const fs = require("fs");
const config = require("../../modules/config");
const { mysql, notification, sendgridMail, slack } = require("../../modules");

async function getRewardExpiry(time, locale) {
    const sql = "select * from (select wt.student_id, s.gcm_reg_id, sum(wte.amount_left) as expiring_amount, wte.status, wte.updated_at, wt.expiry, s.locale from wallet_transaction_expiry wte join wallet_transaction wt on wte.wallet_transaction_id = wt.id join students s on s.student_id = wt.student_id where wt.expiry = ? and wte.status = 'ACTIVE' and s.locale = ? group by wt.student_id order by wallet_transaction_id desc) as wallet_transaction_meta where expiring_amount >= 10";
    return mysql.pool.query(sql, [time, locale]).then((res) => res[0]);
}

async function getNotificationData(locale, amount) {
    let title = `Aapka ₹ ${amount} Reward Cash aaj expire ho raha hai!`;
    let message = "Apna wallet balance check kare and jaldi course purchase kare!💵💵💵";
    if (locale == "hi") {
        title = `आपका ₹ ${amount} रिवॉर्ड कैश आज समाप्त हो रहा है!`;
        message = "अपना वॉलेट बैलेंस चेक करे और जल्दी कोर्स खरीद करे!💵💵💵";
    }

    const data = {
        expiring_amount: amount,
    };
    return {
        notificationData: {
            event: "wallet_page",
            title,
            message,
            firebase_eventtag: "REWARD_EXPIRY_TODAY",
            data: JSON.stringify(data),
        },
    };
}

async function rewardHelper(time, locale) {
    try {
        const rewardExpiry = await getRewardExpiry(time, locale);
        const rewardExpiryObj = rewardExpiry.reduce((res, arr) => {
            res[arr.expiring_amount] = res[arr.expiring_amount] || [];
            res[arr.expiring_amount].push(arr);
            return res;
        }, {});

        const notifData = [];
        for (const key in rewardExpiryObj) {
            if (rewardExpiryObj.hasOwnProperty(key)) {
                const user = [];
                rewardExpiryObj[key].forEach((obj) => {
                    user.push({ id: obj.student_id, gcmId: obj.gcm_reg_id });
                });
                // eslint-disable-next-line no-await-in-loop
                const payload = await getNotificationData(locale, key);
                const notificationInfo = payload.notificationData;
                notifData.push({ user, notificationInfo });
                // eslint-disable-next-line no-await-in-loop
                await notification.sendNotification(user, notificationInfo);
            }
        }
        await fs.appendFileSync(`REWARD_EXPIRY_TODAY_${moment().format("DD-MM-YYYY")}.txt`, `${JSON.stringify(notifData, null, 2)}\n`);
    } catch (e) {
        console.log(e);
        throw new Error(JSON.stringify(e));
    }
}

async function start(job) {
    const fromEmail = "autobot@doubtnut.com";
    const toEmail = "prakher.gaushal@doubtnut.com";
    const ccList = ["dipankar@doubtnut.com", "prashant.gupta@doubtnut.com"];
    const blockNew = [];
    try {
        const time = moment().add(5, "hours").add(30, "minutes").endOf("day")
            .format("YYYY-MM-DD HH:mm:ss");
        await Promise.all([rewardHelper(time, "en"), rewardHelper(time, "hi")]);
        await sendgridMail.sendMail(fromEmail, toEmail, "CRON | REWARD EXPIRY TODAY cron ran successfully", "Daily Report - reward-expiry-today", [`REWARD_EXPIRY_TODAY_${moment().format("DD-MM-YYYY")}.txt`], ccList);
        return { err: null, data: null };
    } catch (e) {
        console.log("e1", e);
        await sendgridMail.sendMail(fromEmail, toEmail, "CRON | ALERT!!! Exception in reward-expiry-today", JSON.stringify(e), [], ccList);
        blockNew.push({
            type: "section",
            text: { type: "mrkdwn", text: `CRON | ALERT!!! Exception in reward-expiry-today <@U01MJU54A21> <@U0273ABLEPL> <@ULGN432HL>:\n\`\`\`${e.stack}\`\`\`` },
        });
        await slack.sendMessage("#payments-team", blockNew, config.paymentsAutobotSlackAuth);
        return { e };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "07 10 * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
