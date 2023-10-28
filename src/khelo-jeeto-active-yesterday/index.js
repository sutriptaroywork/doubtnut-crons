const moment = require("moment");
const _ = require("lodash");
const { redshift, mysql, kafka } = require("../../modules");

/** This cron is to send notification to users who have played a game yesterday */

const LAST_ACTIVE = 1;
const NOTIF_CONTENT = [{
    title: "Kaun hai topics pe expert?",
    message: "Jaanne ke liye try Khelo and Jeeto with friends!",
    image: "https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/46B3EB75-1440-8DA0-97AF-B83613528970.webp",
}, {
    title: "Kya aap Top 3 rank mein ho Khelo aur Jeeto mein?",
    message: "Abhi check karo and firse khelo!",
    image: "https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/2B19639D-48CB-48EA-3313-F0A3BAC3763C.webp",
}];

function getStudentData(studentIds) {
    /* Get student gcm_reg_id */
    const sql = "SELECT student_id, gcm_reg_id FROM students WHERE student_id IN (?) AND gcm_reg_id IS NOT NULL";
    return mysql.pool.query(sql, [studentIds])
        .then((res) => res[0]);
}

async function sendNotification(studentDetails) {
    const studentIds = studentDetails.map((x) => x.student_id);
    studentDetails = await getStudentData(studentIds);

    const notificationMsgIndex = _.random(0, NOTIF_CONTENT.length - 1);
    const notificationData = {
        event: "khelo_jeeto",
        title: NOTIF_CONTENT[notificationMsgIndex].title,
        message: NOTIF_CONTENT[notificationMsgIndex].message,
        image: NOTIF_CONTENT[notificationMsgIndex].image,
        firebase_eventtag: "khelo_jeeto_active_yesterday",
        data: {},
        path: "home",
    };

    const notifReceivers = [];
    const gcmRegId = [];
    for (const student of studentDetails) {
        notifReceivers.push(student.student_id);
        gcmRegId.push(student.gcm_reg_id);
    }
    await kafka.sendNotification(notifReceivers, gcmRegId, notificationData);
    return true;
}

function getApplicableStudents() {
    const lastActiveDate = moment().add(5, "hours").add(30, "minutes").subtract(LAST_ACTIVE, "days")
        .format("YYYY-MM-DD");
        /* This query will get student_id who have played a game yesterday */
    const sql = `SELECT DISTINCT(inviter_id) as student_id FROM dms.khelo_jeeto_result WHERE created_at BETWEEN '${lastActiveDate} 00:00:00' AND '${lastActiveDate} 23:59:59'`;
    return redshift.query(sql).then((res) => res);
}

async function start(job) {
    job.progress(10);
    console.log("task started");
    const applicableStudents = await getApplicableStudents();
    console.log("applicableStudents length", applicableStudents.length);
    job.progress(30);
    let studentDetails; const chunk = 500;
    for (let i = 0, j = applicableStudents.length; i < j; i += chunk) {
        studentDetails = applicableStudents.slice(i, i + chunk);
        sendNotification(studentDetails);
    }
    console.log("task completed");
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "5 14 * * *", // At 02:05 PM
};
