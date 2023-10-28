/* eslint-disable no-await-in-loop */
const moment = require("moment");
const { mysql, kafka, config } = require("../../modules/index");

async function getStudentsThatSubscribed(startTime, endTime) {
    const sql = "SELECT distinct s.*,p.assortment_id from (select * from student_package_subscription where created_at > ? and created_at < ? and amount >=0 )  as sps left join package p on p.id = sps.new_package_id left join course_details cd on cd.assortment_id=p.assortment_id left join students s on sps.student_id = s.student_id where  cd.assortment_type ='course'";
    return mysql.pool.query(sql, [startTime, endTime]);
}

const notificationTemplate = {
    notification: {
        title: "Padho aur jeeto",
        firebaseTag: "PADHO_AUR_JEETO_NEW_STUDENT_24_HOUR_NOTIFICATION",
        image_url: `${config.staticCDN}engagement_framework/4D16D260-A0FA-62EF-A573-EA957CC714F2.webp`,
    },
};

async function sendNotification(students) {
    for (let i = 0; i < students.length; i++) {
        const row = students[i];
        const notificationPayload = {
            event: "leaderboard",
            image: notificationTemplate.notification.image_url,
            title: notificationTemplate.notification.title,
            message: "Padho aur Jeeto",
            firebase_eventtag: notificationTemplate.notification.firebaseTag,
            s_n_id: notificationTemplate.notification.firebaseTag,
        };
        if (row.gcm_reg_id) {
            notificationPayload.data = JSON.stringify({
                source: "notification",
                assortment_id: row.assortment_id,
                type: "paid_user_championship",
            });

            await kafka.sendNotification(
                [row.student_id], [row.gcm_reg_id],
                notificationPayload,
            );
        }
    }
}

async function start(job) {
    try {
        const startTime = moment().add(5, "hours").add(30, "minutes");
        const endTime = moment().add(5, "hours").add(30, "minutes");
        startTime.startOf("hour");
        endTime.endOf("hour");
        startTime.subtract(1, "days");
        endTime.subtract(1, "days");
        const students = await getStudentsThatSubscribed(startTime.format("YYYY-MM-DD HH:mm:ss"), endTime.format("YYYY-MM-DD HH:mm:ss"));
        console.log(students[0]);
        sendNotification(students[0]);
    } catch (e) {
        console.error(e);
    }
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "30 */1 * * *", // at 30th minute of every hour
};
