const { redis } = require("../../modules/index");
const { redshift } = require("../../modules");

async function getTestIds() {
    const sql = "select test_id from classzoo1.testseries where unpublish_time < CURRENT_TIMESTAMP and is_active = 1 and test_id not in (select test_id from classzoo1.scholarship_exam)";
    const users = await redshift.query(sql).then((res) => res);
    return users;
}

async function getTestWiseResults(testId) {
    const sql = `select * from classzoo1.testseries_student_reportcards where test_id = ${testId} and student_id not in (select student_id from classzoo1.internal_subscription)`;
    const users = await redshift.query(sql).then((res) => res);
    return users;
}

async function setTestLeaderboard(testId, marks, studentID) {
    return redis
        .multi()
        .zadd(`leaderboard:tests:${testId}`, marks, studentID)
        .expireat(`leaderboard:tests:${testId}`, parseInt((+new Date()) / 1000) + 60 * 60 * 24 * 3)
        .execAsync();
}

async function start(job) {
    try {
        const completedTestIds = await getTestIds();
        const testIds = [];
        completedTestIds.forEach((e) => {
            testIds.unshift(e.test_id);
        });
        for (let i = 0; i < testIds.length; i++) {
            // eslint-disable-next-line no-await-in-loop
            const results = await getTestWiseResults(testIds[i]);
            if (results && results[0]) {
                for (let j = 0; j < results.length; j++) {
                    setTestLeaderboard(testIds[i], results[j].totalscore, results[j].student_id);
                }
            }
            const newLocal = (i + 1) * 100;
            // eslint-disable-next-line no-await-in-loop
            await job.progress(parseInt((newLocal) / testIds.length));
        }
        job.progress(100);
        return {
            data: {
                done: true,
            },
        };
    } catch (err) {
        console.log(err);
        return {
            err,
        };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "45 2 * * *", // * 2:45 at night everyday
};
