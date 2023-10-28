const {
    mysql, kafka,
} = require("../../modules");

const CONTEST_START_DATE = "2022-01-28";

function getAllStudents(startDate) {
    const sql = `SELECT  pes.student_id, st.gcm_reg_id from practice_english_sessions pes 
    left join students st on pes.student_id = st.student_id 
           where pes.created_at > ? and st.gcm_reg_id is not null group by pes.student_id`;
    return mysql.pool.query(sql, [startDate]).then((res) => res[0]);
}

async function sendNotification(allUsers) {
    const allSids = [];
    const allGCMids = [];
    const notificationPayload = {
        title: "Aaj nahi karni English bolne ki Practice? 🤨",
        message: "Roz practice karne se hi hoti hai English mazboot!! 🗣️",
        firebase_eventtag: "english_daily_reminder",
        event: "english_daily_reminder",
        data: {
            deeplink: "doubtnutapp://practice_english",
        },
    };
    for (let i = 0; i < allUsers.length; i++) {
        allSids.push(allUsers[i].student_id);
        allGCMids.push(allUsers[i].gcm_reg_id);
    }
    return kafka.sendNotification(
        allSids,
        allGCMids,
        notificationPayload,
    );
}

async function start(job) {
    try {
        job.progress(0);

        const allStudents = await getAllStudents(CONTEST_START_DATE);

        console.log(allStudents.length);

        await sendNotification(allStudents);
    } catch (e) {
        console.error(e);
    }
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 15 * * *", // Daily at 3.00 PM
};
