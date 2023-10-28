/* eslint-disable no-await-in-loop */
const moment = require("moment");
const { kafka } = require("../../modules");
const { mysql } = require("../../modules");
const { staticData } = require('./constants');

async function getAllDetailsRegisteredStudents(fromTime, toTime) {
    const sql = `select * from students where gcm_reg_id != '' and timestamp between '${fromTime}' and '${toTime}'`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function start(job) {
    try {
        const notificationData = {
            event: "dnr",
            title: "डाउटनट रुप्या (DNR)",
            message: "डाउटनट पे पढाई करके,अनलॉक करो रिवार्ड्स और रिडीम करो डाउटनट रुप्या (DNR) के साथ",
            image: "https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/B1A1149D-F004-6FBD-CAD2-88215E16C158.webp",
            firebase_eventtag: "dnr_d0",
            data: {},
            path: "home",
        };
        const currentDate = moment().add(5, "hours").add(30, "minutes").format("YYYY-MM-DD");
        // approx 20k per day app installs with valid gcm_id
        const studentsDetails = await getAllDetailsRegisteredStudents(`${currentDate} 00:00:00`, `${currentDate} 23:59:59`);
        job.progress(40);
        const notificationRecievers = [];
        const gcmRegId = [];
        for (let i = 0; i < studentsDetails.length; i++) {
            if (studentId < staticData.dnrExpStartingSid || (studentId >= staticData.dnrExpStartingSid && studentId % 2 !== 0)) {
                notificationRecievers.push(studentsDetails[i].student_id);
                gcmRegId.push(studentsDetails[i].gcm_reg_id);
            }
        }
        job.progress(80);
        await kafka.sendNotification(notificationRecievers, gcmRegId, notificationData);
        job.progress(100);
        return {
            data: {
                done: true,
            },
        };
    } catch (err) {
        console.log(err);
        return { err };
    }
}
module.exports.start = start;
module.exports.opts = {
    cron: "10 18 * * *",
};
