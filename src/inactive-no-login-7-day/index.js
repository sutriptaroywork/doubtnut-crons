const _ = require("lodash");
const notification = require("../../modules/notification");
const { mysql } = require("../../modules");
const flagr = require("../../modules/flagr");

const notificationType = "inactive_no_login_7_day";

function getStudents() {
    const sql = "SELECT id, gcm_reg_id, created_at from pre_login_onboarding where created_at >= date_sub(CURRENT_TIMESTAMP, INTERVAL 7 DAY) AND created_at < date_sub(CURRENT_TIMESTAMP, INTERVAL 10079 MINUTE) AND is_converted = 0 AND gcm_reg_id <> '' ORDER BY created_at DESC";
    return mysql.pool.query(sql).then((res) => res[0]);
}
function getNotificationDataFromType(firebaseEventType) {
    const sql = "SELECT type,title,message,image FROM new_users_notifications where type = ?";
    return mysql.pool.query(sql, [firebaseEventType]).then((res) => res[0]);
}

async function start(job) {
    job.progress(10);
    const students = await getStudents();
    job.progress(30);

    const firebaseEventTag = "NEWLOGINDAY7";
    const notificationData = await getNotificationDataFromType(firebaseEventTag);

    if (!_.isEmpty(notificationData)) {
        const notificationPayload = {
            event: "login",
            title: notificationData[0].title,
            message: notificationData[0].message,
            image: notificationData[0].image,
            s_n_id: notificationType,
            firebase_eventtag: firebaseEventTag,
            data: {
                hour: "Every 1 minute",
            },
        };
        job.progress(60);
        const studentsData = [];
        let counter = 0;

        for (let i = 0; i < students.length; i++) {
            const flgrData = { body: { capabilities: { "otp-send": {} }, entityId: students[i].id.toString() } };
            const flgrResp = await flagr.getFlagrResp(flgrData);
            if (flgrResp !== undefined && flgrResp !== {} && flgrResp["otp-send"] !== undefined && flgrResp["otp-send"].enabled && flgrResp["otp-send"].payload.enabled) {
                const student = {};
                student.id = students[i].id;
                student.gcmId = students[i].gcm_reg_id;
                studentsData.push(student);
                counter++;
            }
        }
        await notification.sendNotification(studentsData, notificationPayload);
        console.log(`total user count ::: ${counter}`);

        job.progress(90);
        console.log("task completed");
        job.progress(100);
    }
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "*/1 * * * *", // run on every minute
};
