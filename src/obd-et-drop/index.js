const moment = require("moment");
const _ = require("lodash");
const { kaleyra, mysql } = require("../../modules");
const redis = require("../../modules/redis");

async function getStudentsTags(date) {
    const sql = "SELECT student_id, tagg from paid_red_flag_users WHERE created_at =?";
    return mysql.pool.query(sql, [date]).then((res) => res[0]);
}

async function getStudentMobile(studentList) {
    const sql = "SELECT mobile from students WHERE student_id in (?)";
    return mysql.pool.query(sql, [studentList]).then((res) => res[0]);
}

async function setStudentLastUsed(studentId, date) {
    return redis.hsetAsync(`USER:PROFILE:${studentId}`, "lastEtDropTime", date);
}

async function getStudentLastUsed(studentId) {
    return redis.hgetAsync(`USER:PROFILE:${studentId}`, "lastEtDropTime");
}

async function start(job) {
    try {
        const date = moment().subtract(1, "days").format("YYYY-MM-DD");
        // ET Drop
        const data = await getStudentsTags(date);
        const groupedData = _.groupBy(data, "student_id");
        const allStudentIds = Object.keys(groupedData);
        let finalStudentsList = [];
        console.log('here1');
        for (let i = 0; i < allStudentIds.length; i++) {
            const studentId = allStudentIds[i];
            const lastUsedDate = await getStudentLastUsed(studentId);
            const toSend = !lastUsedDate || moment(date).diff(lastUsedDate, "days") >= 3;
            if (groupedData[studentId].map((item) => item.tagg).every((item) => item === "2_day_0ET") && toSend) {
                finalStudentsList.push(studentId);
                await setStudentLastUsed(studentId, date);
            }
            if (i % 1000 === 0) {
                console.log(`nth ${i / 1000}`);
            }
        }

        finalStudentsList = finalStudentsList.map(Number);
        console.log('here2');

        let studentMobiles;
        if (finalStudentsList.length > 0) {
            console.log({ finalStudentsList }); 
            const pageCount = Math.floor(finalStudentsList.length / 100);
            studentMobiles = (await getStudentMobile(finalStudentsList)).map((item) => item.mobile);
            const audio = "158376.ivr";
            for (let i = 0; i < pageCount; i++) {
                console.log(`page:${i}`);
                const lastIndex = (i + 1) * 100;
                const firstIndex = i * 100;
                console.log(studentMobiles.slice(firstIndex, lastIndex).join(), audio);
                console.log(await kaleyra.OBDcall(studentMobiles.slice(firstIndex, lastIndex).join(), audio));
            }
        }
        console.log(`called ${finalStudentsList.length} students`);
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
    cron: "30 12 * * *",
};