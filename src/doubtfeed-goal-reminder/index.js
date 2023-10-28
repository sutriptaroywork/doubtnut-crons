/* eslint-disable no-return-await */
const moment = require("moment");
const { config, kafka, mysql } = require("../../modules");

async function sendNotification(studentDetails) {
    for (let i = 0; i < studentDetails.length; i++) {
        if (studentDetails[i] && studentDetails[i].student_id && studentDetails[i].gcm_reg_id) {
            const notificationData = {
                event: "doubt_feed",
                title: studentDetails[i].locale === "hi" ? "सिर्फ 2 घंटे हैं आपके पास!" : "Sirf 2 hrs hain aapke paas!",
                message: studentDetails[i].locale === "hi" ? "आज का गोल पूरा करने में!" : "Aaj ka Goal complete karne mein!",
                image: `${config.staticCDN}engagement_framework/F9705492-E808-29C7-6CD3-4260D63584A4.webp`,
                firebase_eventtag: "dailygoal_end_reminder",
                data: {},
            };
            if (studentDetails[i].version_code >= 921) {
                notificationData.event = "doubt_feed_2";
                notificationData.image = `${config.staticCDN}daily_feed_resources/daily-goal-waiting.webp`;
            }
            await kafka.sendNotification([studentDetails[i].student_id], [studentDetails[i].gcm_reg_id], notificationData);
        }
    }
}

async function getApplicableStudents() {
    const currDate = moment().format("YYYY-MM-DD");
    const sql = `SELECT a.sid AS student_id, s.gcm_reg_id, s.locale, s.is_online AS version_code FROM (SELECT d.sid FROM daily_doubt d 
                 INNER JOIN daily_doubt_resources r ON r.topic_reference = d.id AND r.is_viewed = 0 
                 WHERE d.date BETWEEN '${currDate} 00:00:00' AND '${currDate} 21:30:00' GROUP BY d.sid) AS a 
                 INNER JOIN students s ON a.sid = s.student_id`;
    return await mysql.pool.query(sql).then((res) => res[0]);
}

async function start(job) {
    job.progress(10);
    console.log("task started");
    const dailyGoalStudents = await getApplicableStudents();
    job.progress(30);
    sendNotification(dailyGoalStudents);
    console.log("task completed");
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 22 * * *",
};
