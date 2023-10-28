/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const { mysql, kafka } = require("../../modules");

/** This cron is to send notification to user who have not joined study groups */

const APPLICABLE_VERSION_CODE = 898;

async function sendNotification(studentDetails) {
    const notificationData = {
        event: "study_group",
        title: "Aapke dost Kar rahe hain group Study!",
        message: "Aap bhi banao apna study group aaj hi!",
        image: null,
        firebase_eventtag: "studygroup_non_member",
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

function getStudentDetails(offset, limit) {
    /** Get student gcm_reg_id for all students having version_code >= APPLICABLE_VERSION_CODE */
    const sql = "SELECT student_id, gcm_reg_id FROM students WHERE gcm_reg_id IS NOT NULL AND is_online >= ? LIMIT ? OFFSET ?";
    return mysql.pool.query(sql, [APPLICABLE_VERSION_CODE, limit, offset])
        .then((res) => res[0]);
}

async function getStudyGroupMembers() {
    /** This query will get student_id who are member of study group */
    const sql = "SELECT student_id FROM study_group_members WHERE is_active = 1";
    const studentData = await mysql.pool.query(sql).then((res) => res[0]);
    return _.uniq(studentData.map((x) => x.student_id));
}

async function start(job) {
    job.progress(10);
    console.log("task started");
    const studyGroupMembers = await getStudyGroupMembers();
    job.progress(30);
    const chunk = 200; let skip = 0;
    for (let i = 0, j = 500000; i < j; i += chunk) {
        const allStudents = await getStudentDetails(skip, chunk);
        if (!allStudents.length) {
            console.log("No more data left");
            break;
        }
        const applicableStudents = allStudents.filter((x) => !_.includes(studyGroupMembers, x.student_id));
        sendNotification(applicableStudents);
        skip += chunk;
    }
    console.log("task completed");
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "3 7 */3 * *", // At 07:03 AM, every 3 days
};
