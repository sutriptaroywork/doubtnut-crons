/* eslint-disable no-await-in-loop */
const request = require("request");
const { mysql } = require("../../modules");
const config = require("../../modules/config");

const NOTIFS_PER_ITERATION = 500;

const plans = [
    {
        dayInterval: 0, // in Days
        hourInterval: 3, // in Hours
        notificationInfo: {
            event: "camera",
            title: "Enhance your SAT Practice!",
            message: "Get step by step explanation from video solutions of over 50+ Solved Practice Tests!",
            image_url: "https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/0B59D7A3-0BDC-E180-A9B0-BF2132D09ADD.webp",
            firebase_eventtag: "SATD001",
            s_n_id: "SATD001",
            data: JSON.stringify({
                image_url: "https://d10lpgp6xz60nq.cloudfront.net/q-thumbnail/153285016.png",
            }),
        },
    },
    {
        dayInterval: 1, // in Days
        hourInterval: 3, // in Hours
        notificationInfo: {
            event: "camera",
            title: "Ace your SAT Score!",
            message: "Clear all your SAT Doubts with detailed Chapter-wise Video Solutions!",
            firebase_eventtag: "SATD101",
            s_n_id: "SATD101",
            image_url: "https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/653E4112-6152-271B-665E-402E9104AFCD.webp",
            data: JSON.stringify({
                image_url: "https://d10lpgp6xz60nq.cloudfront.net/q-thumbnail/153285023.png",
            }),
        },
    },
    {
        dayInterval: 2, // in Days
        hourInterval: 3, // in Hours
        notificationInfo: {
            event: "camera",
            title: "Score higher on your ACT!",
            message: "Watch detailed Chapter-wise Video Solutions and clear all your ACT Concepts",
            firebase_eventtag: "SATD201",
            s_n_id: "SATD201",
            image_url: "https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/BF0BA965-1D94-7316-5C1F-F5ADE9D4331D.webp",
            data: JSON.stringify({
                image_url: "https://d10lpgp6xz60nq.cloudfront.net/q-thumbnail/147177002.png",
            }),
        },
    },
];

async function getNewUsers(lower, upper) {
    const sql = `SELECT * from classzoo1.students where clevertap_id = "US_APP" AND timestamp BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL ${lower} HOUR) AND DATE_SUB(CURRENT_DATE, INTERVAL ${upper} HOUR)`;
    console.log(sql);
    const newUsers = await mysql.pool.query(sql).then((res) => res[0]);
    return newUsers;
}

async function sendNotification(user, notificationInfo) {
    const options = {
        method: "POST",
        url: config.newtonUrl,
        headers: { "Content-Type": "application/json" },
        body: { notificationInfo, user },
        json: true,
    };

    // console.log(options);
    return new Promise((resolve, reject) => {
        try {
            request(options, (error, response, body) => {
                if (error) console.log(error);
                console.log(body);
                resolve();
            });
        } catch (err) {
            // fn(err);
            console.log(err);
            reject(err);
        }
    });
}

async function start(job) {
    const logs = {
        results: [],
    };
    try {
        for (let i = 0; i < plans.length; i++) {
            const plan = plans[i];
            const lowerInterval = plan.dayInterval * 24 + plan.hourInterval;
            const upperInterval = plan.dayInterval * 24;
            // eslint-disable-next-line no-await-in-loop
            const users = await getNewUsers(lowerInterval, upperInterval);
            // console.log(i, lowerInterval, upperInterval, "no of users", users.length);
            // eslint-disable-next-line prefer-destructuring
            const notificationInfo = plan.notificationInfo;
            let sendTo = [];
            for (let j = 0; j < users.length; j++) {
                if (users[j].student_id && users[j].gcm_reg_id) {
                    sendTo.push({
                        id: users[j].student_id,
                        gcmId: users[j].gcm_reg_id,
                    });
                }
                if ((!(j % NOTIFS_PER_ITERATION) || (j === (users.length - 1))) && sendTo.length) {
                    // console.log("i", i + 1, "j", j, "SENDING NOTIF FOR SAMPLE QUESTION:", sendTo.length, notificationInfo.title);
                    // eslint-disable-next-line no-await-in-loop
                    await sendNotification(sendTo, notificationInfo);
                    sendTo = [];
                }
            }
            logs.results.push({
                planType: notificationInfo.firebase_eventtag,
                NumberOfNotifsSent: users.length,
            });
            await job.progress(((i + 1) / plans.length) * 100);
        }
        return { data: logs };
    } catch (err) {
        // console.log(e);
        return { err };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 */3 * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
