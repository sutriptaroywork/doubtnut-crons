/* eslint-disable prefer-const */
/* eslint-disable no-await-in-loop */
const redisCacheHelper = require("./helper");

async function start(job) {
    await redisCacheHelper.generateQuizNotificationData();
    await redisCacheHelper.generateQuizNotificationDefaultData();
    await redisCacheHelper.generateECMData();

    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "30 */3 * * *", // Every 3 hrs at 30 min
};
