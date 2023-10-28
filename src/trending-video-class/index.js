/* eslint-disable camelcase */
const moment = require("moment");

const mysql = require("../../modules/mysql");
const redisClient = require("../../modules/redis");

function setByKey(key, value, ttl) {
    return redisClient.multi()
        .set(key, JSON.stringify(value))
        .expire(key, ttl)
        .execAsync();
}

async function getTrendingQuesId(student_class, playlist_from_date, playlist_to_date) {
    try {
        const sql = `select vvs.question_id, count(vvs.question_id) as total
        from video_view_stats vvs inner join questions q on q.question_id = vvs.question_id
        where vvs.created_at between ? and ? and vvs.source = 'android' AND q.class = ? AND q.student_id not in (81,80,94,100,93)
        group by vvs.question_id order by total desc limit 100`;
        const result = await mysql.pool.query(sql, [playlist_from_date, playlist_to_date, student_class])
            .then((res) => res[0]);
        return result;
    } catch (e) {
        console.error(e);
    }
}

async function getTrendingVideo() {
    const classes = [6, 7, 8, 9, 10, 11, 12, 13, 14];
    const playlist_date = moment()
        .subtract(1, "days")
        .format("YYYY-MM-DD");
    for (let i = 0; i < classes.length; i++) {
        const trending_ques = await getTrendingQuesId(classes[i], `${playlist_date} 00:00:00`, `${playlist_date} 23:59:59`);
        setByKey(`trending_videos_${classes[i]}`, trending_ques, 86400 * 7);
    }
    return true;
}

async function start(job) {
    await getTrendingVideo();
    return { data: "success" };
}

module.exports.start = start;
module.exports.opts = {
    cron: "48 1 * * *",
};
