const moment = require("moment");
const { redshift, mysql, kafka } = require("../../modules");

const LAST_ACTIVE = 7;

/** This cron is to send notification to users who are inactive in study groups from 7 days */

function getStudentData(studentIds) {
    /* Get student gcm_reg_id */
    const sql = "SELECT student_id, gcm_reg_id FROM students WHERE student_id IN (?) AND gcm_reg_id IS NOT NULL";
    return mysql.pool.query(sql, [studentIds])
        .then((res) => res[0]);
}

async function sendNotification(studentDetails) {
    const studentIds = studentDetails.map((x) => x.student_id);
    studentDetails = await getStudentData(studentIds);

    const notificationData = {
        event: "study_group",
        title: "Study groups mein padhai start kyu nai ki?",
        message: "Aaj hi karo apne friends ko messsage  and share karo notes!",
        image: null,
        firebase_eventtag: "studygroup_inactive_weekly",
        data: { is_study_group_exist: false },
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
    const lastActiveTimestamp = moment().add(5, "hours").add(30, "minutes").subtract(LAST_ACTIVE, "days")
        .format("YYYY-MM-DD HH:mm:ss");
        /* This query will get student_id who are not active from 7 days */
    const sql = `SELECT  a.student_id FROM ((SELECT DISTINCT student_id FROM classzoo1.study_group_members
                 WHERE is_active = 1) AS a LEFT JOIN (SELECT student_id, COUNT(message) AS message_count, 
                 MAX(TO_DATE(created_at,'YYYY|MM|DD')) as date FROM analytics.chatroom_messages 
                 WHERE room_type ='study_group' AND (message LIKE '%text_widget%' OR message LIKE '%widget_audio_player%' 
                 OR message LIKE '%image_card%' OR message LIKE '%widget_pdf_view%') GROUP BY 1) AS b 
                 ON a.student_id = b.student_id ) WHERE message_count IS NOT NULL AND date <= '${lastActiveTimestamp}'`;
    return redshift.query(sql).then((res) => res);
}

async function start(job) {
    job.progress(10);
    console.log("task started");
    const applicableStudents = await getApplicableStudents();
    console.log("applicableStudents length", applicableStudents.length);
    job.progress(30);
    let i; let j; let studentDetails; const chunk = 10000;
    for (i = 0, j = applicableStudents.length; i < j; i += chunk) {
        studentDetails = applicableStudents.slice(i, i + chunk);
        sendNotification(studentDetails);
    }
    console.log("task completed");
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "7 7 */3 * *", // At 07:07 AM, every 3 days
};
