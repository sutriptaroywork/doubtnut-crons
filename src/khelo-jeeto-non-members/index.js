/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const { mysql, redshift, kafka } = require("../../modules");

/** This cron is to send notification to user who have not played khelo jeeto */

const APPLICABLE_VERSION_CODE = 916;
const NOTIF_CONTENT = [{
    title: "Try Kiya Naya Khelo and Jeeto Game?",
    message: "Khelo Game and Jeeto Dher Saare rewards!",
    image: "https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/3DA08215-D849-B8A8-469B-C16A1608797C.webp",
}, {
    title: "50,000+ bacche khel chuken hein topics pe game aaj.",
    message: "Try karo aap bhi and dekho kaun hai master!",
    image: "https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/906149A0-7BEA-7B95-ABEA-FBB47877BDE8.webp",
}, {
    title: "Kaun hai topics pe expert?",
    message: "Jaanne ke liye try Khelo and Jeeto with friends!",
    image: "https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/46B3EB75-1440-8DA0-97AF-B83613528970.webp",
}, {
    title: "Revision pe awards?",
    message: "Ab Khelo and Jeeto pe milenge padhne ke liye dher saare awards, Check now!",
    image: null,
}];

async function sendNotification(studentDetails) {
    const notificationMsgIndex = _.random(0, NOTIF_CONTENT.length - 1);
    const notificationData = {
        event: "khelo_jeeto",
        title: NOTIF_CONTENT[notificationMsgIndex].title,
        message: NOTIF_CONTENT[notificationMsgIndex].message,
        image: NOTIF_CONTENT[notificationMsgIndex].image,
        firebase_eventtag: "khelo_jeeto_non_members",
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

function getStudentDetails(offset, limit) {
    /** Get student gcm_reg_id for all students having version_code >= APPLICABLE_VERSION_CODE */
    const sql = "SELECT student_id, gcm_reg_id FROM students WHERE gcm_reg_id IS NOT NULL AND is_online >= ? LIMIT ? OFFSET ?";
    return mysql.pool.query(sql, [APPLICABLE_VERSION_CODE, limit, offset])
        .then((res) => res[0]);
}

async function getKheloJeetoMembers() {
    /** This query will get student_id who have not played khelo jeeto */
    const sql = "SELECT student_id FROM dms.khelo_jeeto_student_rewards";
    const studentData = await redshift.query(sql).then((res) => res);
    return studentData.map((x) => x.student_id);
}

async function start(job) {
    job.progress(10);
    console.log("task started");
    const kheloJeetoMembers = await getKheloJeetoMembers();
    job.progress(30);
    const chunk = 200; let skip = 0;
    for (let i = 0, j = 500000; i < j; i += chunk) {
        const allStudents = await getStudentDetails(skip, chunk);
        if (!allStudents.length) {
            console.log("No more data left");
            break;
        }
        const applicableStudents = allStudents.filter((x) => !_.includes(kheloJeetoMembers, x.student_id));
        sendNotification(applicableStudents);
        skip += chunk;
    }
    job.progress(100);
    console.log("task completed");
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "14 14 */2 * *", // At 02:14 PM on every 2nd day
};
