const {
    redis, mysql,
} = require("../../modules");

function getAllQuestions() {
    const sql = "select q.question_id, ts.type as question_type from questions q left join text_solutions ts on q.question_id = ts.question_id WHERE q.student_id = -501 and q.is_skipped = 0 and ts.type not in ('translate_hi','translate_en')";
    return mysql.pool.query(sql, []).then((res) => res[0]);
}

async function start(job) {
    try {
        job.progress(0);
        const allQuestions = await getAllQuestions();

        console.log("Total Questions: ", allQuestions.length);
        await redis.setAsync("PracticeEnglish:TOTAL_QUESTIONS", allQuestions.length, "Ex", 60 * 60 * 24);
        for (let i = 1; i <= 50; i++) {
            const randomQuesSet = allQuestions.sort(() => Math.random() - Math.random());
            const redisKey = `PracticeEnglish:QUESTIONS_SET:${i}`;
            console.log("setting: ", redisKey);
            console.log("firstQues: ", randomQuesSet[0]);
            // eslint-disable-next-line no-await-in-loop
            await redis.delAsync(redisKey);
            // eslint-disable-next-line no-await-in-loop
            await redis.multi()
                .lpush(redisKey, ...randomQuesSet.map((obj) => JSON.stringify(obj)))
                .expire(redisKey, 60 * 60 * 24)
                .execAsync();
        }
    } catch (e) {
        console.error(e);
    }
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 17 * * *", // Daily at 5.00 PM
};
