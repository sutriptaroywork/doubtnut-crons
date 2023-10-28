const { redshift, mysql, kafka } = require("../../modules");

/** This cron is to send notification to user who have joined the study group but haven't send any message */

function getStudentData(studentIds) {
    /** Get student gcm_reg_id */
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
        message: "Aaj hi karo apne friends ko message  and share karo notes!",
        image: null,
        firebase_eventtag: "studygroup_inactive",
        data: { is_study_group_exist: false },
    };

    const notifReceivers = [];
    const gcmRegIg = [];
    for (const student of studentDetails) {
        notifReceivers.push(student.student_id);
        gcmRegIg.push(student.gcm_reg_id);
    }
    await kafka.sendNotification(notifReceivers, gcmRegIg, notificationData);
    return true;
}

function getApplicableStudents() {
    /** This query will get student_id whose message count is null in type = study_group */
    const sql = `SELECT  a.student_id FROM ((SELECT DISTINCT student_id FROM classzoo1.study_group_members WHERE is_active = 1) AS a LEFT JOIN
                 (SELECT student_id, COUNT(message) AS message_count FROM analytics.chatroom_messages
                 WHERE room_type ='study_group' AND (message LIKE '%text_widget%' OR message LIKE '%widget_audio_player%' OR
                 message LIKE '%image_card%' OR message LIKE '%widget_pdf_view%')  GROUP BY 1) 
                 AS b ON a.student_id = b.student_id) WHERE message_count IS NULL`;
    return redshift.query(sql).then((res) => res);
}

async function start(job) {
    job.progress(10);
    console.log("task started");
    const applicableStudents = await getApplicableStudents();
    console.log("applicableStudents length", applicableStudents.length);
    job.progress(30);
    let studentDetails; const chunk = 10000;
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
    cron: "11 7 */3 * *", // At 07:11 AM, every 3 days
};
