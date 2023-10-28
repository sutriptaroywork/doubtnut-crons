/* eslint-disable prefer-const */
/* eslint-disable no-await-in-loop */
const moment = require("moment");
const { mysql } = require("../../modules");

async function removeRecommendationLogs(startTime, endTime) {
    const sql = "delete from recommendation_message_submit_logs where created_at >= ? and created_at <= ?";
    return mysql.writePool.query(sql, [startTime, endTime]).then((res) => res[0]);
}

async function removeCouponsNew(startTime, endTime) {
    const sql = "delete from coupons_new where created_at >= ? and created_at <= ?";
    return mysql.writePool.query(sql, [startTime, endTime]).then((res) => res[0]);
}

async function removeCourseAds(startTime, endTime) {
    const sql = "delete from course_ads_view_stats_1 where created_at >= ? and created_at <= ?";
    return mysql.writePool.query(sql, [startTime, endTime]).then((res) => res[0]);
}

async function removeCourseAdsEng(startTime, endTime) {
    const sql = "delete from course_ads_engagetime_stats_1 where created_at >= ? and created_at <= ?";
    return mysql.writePool.query(sql, [startTime, endTime]).then((res) => res[0]);
}
async function start(job) {
    const endTimeSeven = moment().subtract(7, 'd').subtract(2, 'h');
    const startTimeSeven = endTimeSeven.subtract(1, 'd');
    await removeRecommendationLogs(startTimeSeven.format(), endTimeSeven.format());

    const endTimeSixty = moment().subtract(60, 'd').subtract(2, 'h');
    const startTimeSixty = endTimeSixty.subtract(1, 'd');
    await removeCouponsNew(startTimeSixty.format(), endTimeSixty.format());

    const endTimeThirty = moment().subtract(30, 'd').subtract(2, 'h');
    const startTimeThirty = endTimeThirty.subtract(1, 'd');
    await removeCourseAds(startTimeThirty.format(), endTimeThirty.format());
    await removeCourseAdsEng(startTimeThirty.format(), endTimeThirty.format());

    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "30 */3 * * *", // Every 3 hrs at 30 min
};
