/* eslint-disable no-await-in-loop */
const moment = require("moment");
const axios = require("axios");
const redis = require("../../modules/redis");
const { mysql, kafka, config } = require("../../modules/index");

async function getPaidUserChampionshipLeaderboardWeekly(weekNumber, assortmentId, min, max) {
    return redis.zrevrange(`padho_aur_jeeto_weekly_leaderboard:${weekNumber}:${assortmentId}`, min, max);
}

async function getPaidUserChampionshipLeaderboardWeeklyScore(weekNumber, assortmentId, studentId) {
    return redis.zscore(`padho_aur_jeeto_weekly_leaderboard:${weekNumber}:${assortmentId}`, studentId);
}

async function getPaidAssortmentIdList() {
    const sql = "SELECT DISTINCT assortment_id from course_details cd where NOW()>= start_date  and NOW()<= end_date  and is_free = 0 and assortment_type  = 'course'";
    return mysql.pool.query(sql).then((x) => x[0]);
}
async function insertTshirtWinners(studentId, winningDate, assortmentId, duration, rank, percentage) {
    const sql = "insert into paid_user_championship_shirt_winners (student_id, winning_date, assortment_id, duration, rank, percentage, reward) values (?,?,?,?,?,?,?) ";
    return mysql.writePool.query(sql, [studentId, winningDate, assortmentId, duration, rank, percentage, "T-Shirt"]);
}

async function start(job) {
    // try {
    //     const now = moment().add(5, "hours").add(30, "minutes");
    //     const lastWeekNumber = now.subtract(1, "week").isoWeek();
    //     const paidAssortmentList = await getPaidAssortmentIdList();
    //     const assortmentList = [];
    //     paidAssortmentList.forEach((item) => assortmentList.push(item.assortment_id));
    //     for (let i = 0; i < assortmentList.length; i++) {
    //         const assortmentId = assortmentList[i];
    //         const winners = await getPaidUserChampionshipLeaderboardWeekly(lastWeekNumber, assortmentId, 0, 2);
    //         for (let j = 0; j < winners.length; j++) {
    //             const percentage = await getPaidUserChampionshipLeaderboardWeeklyScore(lastWeekNumber, assortmentId, winners[j]);
    //             const winningDate = moment().startOf("weekly").format("YYYY-MM-DD HH:mm:ss");
    //             await insertTshirtWinners(winners[j], winningDate, assortmentId, "weekly", j + 1, percentage);
    //         }
    //     }
    //     job.progress(90);
    // } catch (e) {
    //     console.error(e);
    // }
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 0 1 * *", // At 00:00 on first day of month
};
