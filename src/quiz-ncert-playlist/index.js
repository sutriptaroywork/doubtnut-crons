const mysql = require("../../modules/mysql");
const redisClient = require("../../modules/redis");

function setByKey(key, value, ttl) {
    return redisClient.multi()
        .set(key, JSON.stringify(value))
        .expire(key, ttl)
        .execAsync();
}

async function getNcertPlaylistQuestion(student_class) {
    try {
        console.log(`query executing for ${student_class}`);
        const sql = `select question_id, subject, book, chapter, question, question_image from 
                     questions q where q.student_id in (1, 69, -111, -117, 25, 51) and 
                     q.class=? AND q.is_answered = 1 AND q.is_text_answered = 0 order by q.question_id desc limit 1`;
        const result = await mysql.pool.query(sql, [student_class])
            .then((res) => res[0]);
        console.log(`query executed for ${student_class}`);
        return result;
    } catch (e) {
        console.error(e);
    }
}

async function setNcertPlaylist() {
    const available_classes = ["6", "7", "8", "9", "10", "11", "12"];
    for (let i = 0; i < available_classes.length; i++) {
        const result = await getNcertPlaylistQuestion(available_classes[i]);

        const data = {
            question_id: result[0].question_id,
            subject: result[0].subject,
            book: result[0].book,
            chapter: result[0].chapter,
            question: result[0].question,
            question_image: result[0].question_image,
        };
        setByKey(`ncert_playlist_for_${available_classes[i]}`, data, 7 * 86400);
    }
    return true;
}

async function start(job) {
    await setNcertPlaylist();
    return { data: "success" };
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 25 3 * * *",
};
