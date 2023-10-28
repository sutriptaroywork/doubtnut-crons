/* eslint-disable no-await-in-loop */
const { config, mysql } = require("../../modules");

const batchSize = parseInt(process.env.QUESTIONS_NEW_COPY_BATCH_SIZE) || 100;

async function getMaxQidFromQuestions() {
    const sql = "select min(question_id) as question_id from questions_new";
    const { question_id } = await mysql.pool.query(sql).then((res) => res[0][0]);
    return question_id;
}

async function copyData(maxId) {
    const sql = `insert ignore into questions_new select * from questions where student_id > 100 and timestamp>='2021-01-01' and question_id < ${maxId} order by question_id desc limit ${batchSize}`;
    console.log(sql);
    if (!config.prod) {
        return true;
    }
    try {
        const res = await mysql.writePool.query(sql);
        console.log(res);
    } catch (e) {
        console.error(e);
        throw e;
    }
}

async function start(job) {
    const maxId = await getMaxQidFromQuestions();
    await job.progress(50);

    await copyData(maxId);
    console.log("Done", maxId);
    return { data: { maxId } };
}

module.exports.start = start;
module.exports.opts = {
    cron: "*/5 * 22,23,0-5 * * *",
    disabled: true,
};
