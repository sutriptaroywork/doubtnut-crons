const { mysql, kafka } = require("../../modules");

async function getAllAnswers(locale) {
    const sql = `select a.video_language as locale, a.target_group, a.package, b.class, b.chapter, b.subject, b.ocr_text, b.question_id, d.answer_id, d.answer_video from (select * from classzoo1.studentid_package_mapping_new where video_language in ('${locale}') and to_index=1) as a left join classzoo1.questions as b on a.student_id=b.student_id left join (select max(answer_id) as answer_id, question_id from classzoo1.answers group by question_id) as c  on b.question_id=c.question_id left join classzoo1.answers as d on c.answer_id=d.answer_id  where b.is_answered=1 and d.answer_id is not null and d.youtube_id is null and a.package != 'USER_ASKED' and b.matched_question is null order by b.question_id DESC LIMIT 110`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getBooks(locale) {
    const sql = `select distinct(package) as books from studentid_package_mapping_new where video_language='${locale}'`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function start(job) {
    try {
        job.progress(0);
        const [answers, books] = await Promise.all([getAllAnswers("te"), getBooks("te")]);
        job.progress(50);
        let promise = [];
        for (let i = 0; i < answers.length; i++) {
            try {
                if (answers[i].answer_video.includes(".mp4")) {
                    const msgData = {
                        data: {
                            answer: answers[i],
                            books,
                        },
                    };
                    promise.push(kafka.publishRaw("bull-cron.yt-answer.upload", msgData));
                    if ((i % 3 === 0 && i !== 0) || i === answers.length - 1) {
                        // eslint-disable-next-line no-await-in-loop
                        await Promise.all(promise);
                        promise = [];
                    }
                } else {
                    console.log("skip");
                }
            } catch (e) {
                console.log(e);
            }
        }
        job.progress(100);
        return { err: null, data: null };
    } catch (e) {
        console.log(e);
        return { err: e, data: null };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 1 * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
