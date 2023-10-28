/* eslint-disable no-await-in-loop */
const moment = require("moment");
const _ = require("lodash");
const {
    config, redshift, kafka, mysql,
} = require("../../modules");

/** This cron notifies the users who have not generated daily goal till cron timing */

const minVersionAllowed = 904;
const appVersionAllowed = "7.8.255";
const NOTIFICATION_LIMIT = 50000;

async function sendNotification(studentDetails) {
    for (const student of studentDetails) {
        if (student && student.student_id && student.gcm_reg_id) {
            const notificationData = {
                event: "camera",
                title: student.locale === "hi" ? "किसका इंतज़ार कर रहे हैं?" : "Kiska wait kar rahe hain?",
                message: student.locale === "hi" ? "सेट करो आज का लक्ष्य/गोल और शुरू करो पढ़ाई!" : "Set karo aaj ka Goal aur start karo padhai!",
                image: `${config.staticCDN}engagement_framework/58A73B9D-BB80-7793-7240-BB299BD74078.webp`,
                firebase_eventtag: "dailygoal_setup_reminder",
                data: {},
            };
            if (student.version_code >= 921) {
                notificationData.image = `${config.staticCDN}daily_feed_resources/set-daily-goal.webp`;
            }
            await kafka.sendNotification([student.student_id], [student.gcm_reg_id], notificationData);
        }
    }
    return true;
}

function getStudentDetails(studentClass, offset, limit) {
    const sql = `SELECT student_id, gcm_reg_id, locale, is_online AS version_code FROM classzoo1.students WHERE student_class = ${studentClass} AND is_web = 0
                     AND (is_online >= ${minVersionAllowed} OR app_version >= '${appVersionAllowed}') ORDER BY student_id DESC LIMIT ${limit} OFFSET ${offset}`;
    return redshift.query(sql).then((res) => res);
}

async function getDoubtFeedStudents() {
    const currDate = moment().format("YYYY-MM-DD");
    const sql = `SELECT sid FROM daily_doubt WHERE date BETWEEN '${currDate} 00:00:00' AND '${currDate} 14:00:00'`;
    const studentData = await mysql.pool.query(sql).then((res) => res[0]);
    return _.uniq(studentData.map((x) => x.sid));
}

async function sendNotificationClassWise(studentClass, doubtFeedStudents) {
    const chunk = 300;
    for (let i = 0, j = NOTIFICATION_LIMIT; i <= j; i += chunk) {
        const allowedUsers = await getStudentDetails(studentClass, i, chunk);
        if (!allowedUsers.length) {
            console.log("No more data left");
            break;
        }
        const applicableStudents = allowedUsers.filter((item) => !doubtFeedStudents.includes(item.student_id));
        sendNotification(applicableStudents);
    }
    return true;
}

async function start(job) {
    job.progress(10);
    console.log("task started");
    const doubtFeedStudents = await getDoubtFeedStudents();
    job.progress(30);
    const studentClass = [6, 7, 8, 9, 10, 11, 12, 13, 14];
    for (let i = 0; i < studentClass.length; i++) {
        await sendNotificationClassWise(studentClass[i], doubtFeedStudents);
    }
    console.log("task completed");
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 14 * * *", // At 02:00 PM everyday
};
