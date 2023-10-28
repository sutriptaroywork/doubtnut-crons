/* eslint-disable no-await-in-loop */
// const elasticSearch = require("elasticsearch");
const moment = require("moment");
// const { mysql } = require("../../modules");
const redis = require("../../modules/redis");
const { redshift } = require("../../modules");

// const elasticClient = new elasticSearch.Client({
//     host: config.elasticsearch.host3,
//     apiVersion: "7.1",
// });

// const dailyExpiry = 60 * 60 * 24;
// const weeklyExpiry = 60 * 60 * 24 * 7;
const monthlyExpiry = 60 * 60 * 24 * 30;
// const thrityMinutes = 60 * 30;

// async function setLastTimeElasticSearch(timestamp) {
//     return redis.setAsync("question_pucho_contest_lastupdate_timestamp", timestamp);
// }

// async function getLastTimeElasticSearch() {
//     return redis.getAsync("question_pucho_contest_lastupdate_timestamp");
// }

async function getQuestionPuchoContestScore(studentId, date) {
    return redis.zscoreAsync(`question_pucho_contest_leaderboard:${date}`, studentId);
}

// async function getQuestionPuchoContestLifetimeScore(studentId) {
//     return redis.zscoreAsync("question_pucho_contest_lifetime_leaderboard", studentId);
// }

async function setQuestionPuchoContestLeaderboard(studentId, date, points) {
    redis.multi()
        .zadd(`question_pucho_contest_leaderboard:${date}`, points, studentId)
        .expire(`question_pucho_contest_leaderboard:${date}`, monthlyExpiry)
        .execAsync();
}
// async function setQuestionPuchoContestLifetimeLeaderboard(studentId, points) {
//     redis.multi()
//         .zadd("question_pucho_contest_lifetime_leaderboard", points, studentId)
//         .expire("question_pucho_contest_lifetime_leaderboard", weeklyExpiry)
//         .execAsync();
// }

// function delay(ms = 10000) {
//     return new Promise((resolve) => {
//         setTimeout(() => {
//             resolve();
//         }, ms);
//     });
// }

function getAggsData(lowerTimeStamp, upperTimeStamp) {
//     const sql = `SELECT vvs.student_id,
//     count(distinct vvs.parent_id) as count_asked from
//     classzoo1.questions_new qn
//     inner join
//     classzoo1.video_view_stats vvs
//     on qn.question_id =vvs.parent_id
//     where qn.timestamp BETWEEN '${lowerTimeStamp}' and '${upperTimeStamp}' and vvs.created_at BETWEEN '${lowerTimeStamp}' and '${upperTimeStamp}' and source = 'WHA'
//    group by 1`;
    const sql = `SELECT vvs.student_id,
    count(distinct vvs.parent_id) as count_asked from 
    classzoo1.questions_new qn 
    inner join
    classzoo1.video_view_stats vvs 
    on qn.question_id =vvs.parent_id 
    where (qn.curtimestamp+ interval '330 minute') BETWEEN '${lowerTimeStamp}' and '${upperTimeStamp}'
    and (vvs.created_at + interval '330 minutes') BETWEEN '${lowerTimeStamp}' and '${upperTimeStamp}' and source IN ('WHA', 'WHA_new')
   group by 1`;
    console.log(sql);
    return redshift.query(sql);
    // return mysql.pool.query(sql).then((res) => res[0]);
}

async function start(job) {
    try {
        const now = moment().add(5, "hours").add(30, "minutes");
        const date = now.clone().add(2, "hours").format("YYYY-MM-DD");// leaderboard changes at 10 pm
        const upperTimeStamp = now.clone().add(2, "hours").set("hour", 22).set("minute", 0)
            .set("second", 0)
            .format("YYYY-MM-DD HH:mm:ss");
        const lowerTimeStamp = now.clone().add(2, "hours").subtract(1, "day").set("hour", 22)
            .set("minute", 0)
            .set("second", 0)
            .format("YYYY-MM-DD HH:mm:ss");
        // const lowerTimeStamp = "2022-03-30 23:00:00";
        // const upperTimeStamp = "2022-03-31 23:58:00";
        // const date = "2022-03-31";
        // all students who asked questions today until now
        // let lastTimeStamp = Number(await getLastTimeElasticSearch());

        // const currentTimestamp = moment().unix();
        // if (currentTimestamp - lastTimeStamp > thrityMinutes) {
        //     lastTimeStamp = moment().subtract(5, "minutes").unix();
        // }
        // setLastTimeElasticSearch(currentTimestamp);
        // const lowerTimeStamp = moment.unix(lastTimeStamp).add(5, "hour").add(30, "minute").format("YYYY-MM-DD HH:mm:ss");
        // const upperTimeStamp = moment.unix(currentTimestamp).add(5, "hour").add(30, "minute").format("YYYY-MM-DD HH:mm:ss");
        // console.log("timestamps: ", lowerTimeStamp, upperTimeStamp);
        const data = await getAggsData(lowerTimeStamp, upperTimeStamp);
        // console.log("Data: \n", data);
        console.log(data.length);
        for (let i = 0; i < data.length; i++) {
            const studentId = data[i].student_id;
            const score = data[i].count_asked;
            // console.log(studentId, score);
            // eslint-disable-next-line no-await-in-loop
            // const existingScores = await getQuestionPuchoContestScore(studentId, date);
            // const existingScores = await Promise.all([getQuestionPuchoContestScore(studentId, date), getQuestionPuchoContestLifetimeScore(studentId)]);
            const totalScore = score || 0;
            // const lifetimeScore = existingScores[1] ? Number(existingScores[1]) : 0;
            // console.log(studentId, totalScore);
            await Promise.all([setQuestionPuchoContestLeaderboard(studentId, date, totalScore)]);
            const existingScores = await getQuestionPuchoContestScore(studentId, date);
            console.log(studentId, existingScores === totalScore);
            // setQuestionPuchoContestLifetimeLeaderboard(studentId, lifetimeScore + score);
            job.progress((i * 100) / data.length);
        }
        console.log("******** redis entry completed ");
        job.progress(100);
    } catch (e) {
        console.error(e);
    }
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "*/30 * * * *", // every 30 minutes
};
