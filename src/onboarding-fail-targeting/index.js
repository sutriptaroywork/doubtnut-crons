/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const moment = require("moment");
const {
    mysql, redis, notification, deeplink,
} = require("../../modules");

async function fetchUsers() { // 45 ms
    const sql = "select * from pre_login_onboarding where created_at < (NOW() - INTERVAL 4 HOUR) and created_at > (NOW() - INTERVAL 5 HOUR) and is_converted=0 and gcm_reg_id is not null and gcm_reg_id <> ''";
    // console.log(sql);
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getRedirectionDetailsFromCampaign(campaign) {
    const sql = `select * from campaign_redirection_mapping where campaign='${campaign}'`;
    // console.log(sql);
    return mysql.pool.query(sql).then((res) => res[0]);
}

function getNotificationPayload(locale) {
    const obj = {
        hi() {
            return _.shuffle([
                { title: "लॉगइन करना है बेहद ज़रूरी! क्योंकि", message: "स्पोकन इंग्लिश कोर्स कर रहा आपका इंतज़ार!" },
                { title: "डाउटनट करेगा आपकी पर्सनालिटी बूस्ट!", message: "ये स्पोकन इंग्लिश कोर्स करो जॉइन!" },
                { title: "डाउटनट जानता है आपके लिए क्या है बेस्ट!", message: "तो अभी खरीदो स्पोकन इंग्लिश कोर्स!" }])[0];
        },
        en() {
            return _.shuffle([
                { title: "Login fast! You dont want to miss this!", message: "Spoken English course is waiting for you!" },
                { title: "Learn Grow Repeat with Doubtnut!", message: "Here's Spoken English course for you!" },
                { title: "Improve your communication skills!", message: "Buy Spoken English course on Doubtnut!" }])[0];
        },
    };
    return obj[locale]();
}

async function start(job) {
    try {
        const students = await fetchUsers();
        console.log("students");
        console.log(students.length);
        console.log(moment().add(5, "hour").add(30, "minutes").subtract(1, "day")
            .format("YYYY-MM-DD"));
        for (let i = 0; i < students.length; i++) {
            // console.log(students[i]);
            const { udid } = students[i];

            // check campaign
            let campaignData = await redis.hgetAsync(`branch:${moment().add(5, "hour").add(30, "minutes").format("YYYY-MM-DD")}`, `udid_${udid}`);
            if (_.isNull(campaignData)) {
                campaignData = await redis.hgetAsync(`branch:${moment().add(5, "hour").add(30, "minutes").subtract(1, "day")
                    .format("YYYY-MM-DD")}`, `udid_${udid}`);
            }
            if (!_.isNull(campaignData)) {
                // console.log("campaignData");
                // console.log(`udid_${udid}`);
                // console.log(campaignData);
                // console.log(students[i].student_id);
                campaignData = campaignData.split("::")[1];
                // get deeplink from campaign name
                const deeplinkDetails = await getRedirectionDetailsFromCampaign(campaignData);
                // console.log(deeplinkDetails);
                if (deeplinkDetails.length > 0) {
                    if (_.includes([1, 2, 7], deeplinkDetails[0].id)) {
                        if (students[i].locale != "en") {
                            students[i].locale = "hi";
                        }
                        // send notification
                        const d = getNotificationPayload(students[i].locale);
                        const notificationPayload = {
                            event: "camera",
                            image: "",
                            title: d.title,
                            message: d.message,
                            firebase_eventtag: "FAIL_ONBOARDING",
                            s_n_id: "FAIL_ONBOARDING",
                        };
                        // console.log(await notification.sendNotification([{ id: "", gcmId: "fPC-rOStRRmqQzTP9jVC0G:APA91bHzPHm7afF5fuYJ0p3IygnadSCbV_S3KRdDQWEvr4QcGaLz2s--luCRyME5Cs0AMgwVYjcoFYXo1RaJyDn6e2RU0G-4z382ZhJM2PBgy1-DJyVMGHt2mBV9j2YmuX7m5_KMtfLL" }], notificationPayload));
                        console.log(await notification.sendNotification([{ id: "", gcmId: students[i].gcm_reg_id }], notificationPayload));
                    }
                }
            } else {
                console.log("not found");
            }
        }
        job.progress(100);
    } catch (e) {
        console.log("error");
        console.log(e);
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 * * * *",
    removeOnComplete: 10,
    removeOnFail: 20,
};
