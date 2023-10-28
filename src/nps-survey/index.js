const moment = require("moment");
const mysql = require("../../modules/mysql");
const redisClient = require("../../modules/redis");

const dailyExpiry = 60 * 60 * 24;

async function setSurveyStudentId(studentIds, data, job) {
    try {
        for (let i = 0; i < studentIds.length; i++) {
            redisClient.multi()
                .hset("NPS6", studentIds[i], data)
                .expire("NPS6", dailyExpiry)
                .execAsync();
            job.progress(30 + ((60 / studentIds.length) * i));
        }
        return true;
    } catch (e) {
        console.log(e);
        return false;
    }
}

async function getApplicableStudents() {
    try {
        const surveyDay = moment().subtract(1, "day");
        const startTime = surveyDay.startOf("day").add(5, "hours").add(30, "minutes").toISOString()
            .slice(0, 19)
            .replace("T", " ");

        const sql = `SELECT DISTINCT student_id FROM payment_info WHERE status ='SUCCESS' and created_at >= '${startTime}' 
                    AND student_id not in (SELECT student_id from internal_subscription) AND amount + wallet_amount > 1`;
        const result = await mysql.pool.query(sql).then((res) => res[0]);
        return result.map((item) => item.student_id);
    } catch (e) {
        console.log(e);
        return false;
    }
}

async function start(job) {
    try {
        job.progress(10);
        console.log("task started");
        const applicableStudents = await getApplicableStudents();
        job.progress(30);
        await setSurveyStudentId(applicableStudents, 1, job);
        job.progress(100);
        console.log("task completed");
        return { data: "success" };
    } catch (e) {
        console.log(e);
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "5 3 * * *",
};
