/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const moment = require("moment");
const { mysql } = require("../../modules");
const redisClient = require("../../modules/redis");

async function getNewThumbnailAdditions() {
    const sql = "select * from new_thumbnail_experiment where is_active = 1 and updated_at > date_sub(now(), interval 65 minute)";
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getNewThumbnailAdditionAll() {
    const sql = "select * from new_thumbnail_experiment where is_active = 1";
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function start(job) {
    try {
        const startTime = moment().add(5, "hours").add(30, "minutes").startOf("day")
            .add(50, "minutes")
            .format();
        const endTime = moment().add(5, "hours").add(30, "minutes").startOf("day")
            .add(70, "minutes")
            .format();
        let newThumbnailAdditions;
        if (moment().add(5, "hours").add(30, "minutes").isAfter(startTime) && moment().add(5, "hours").add(30, "minutes").isBefore(endTime)) {
            // recreate whole cache once a day
            newThumbnailAdditions = await getNewThumbnailAdditionAll();
        } else {
            newThumbnailAdditions = await getNewThumbnailAdditions();
        }
        if (!_.isEmpty(newThumbnailAdditions)) {
            for (let i = 0; i < newThumbnailAdditions.length; i++) {
                if (newThumbnailAdditions[i].is_active === 0) {
                    await redisClient.delAsync(`NEW_THUMBNAIL_EXPERIMENT:${newThumbnailAdditions[i].old_detail_id}:${newThumbnailAdditions[i].class}:${newThumbnailAdditions[i].question_id}`);
                } else if (newThumbnailAdditions[i].is_active === 1) {
                    const active = 1;
                    await redisClient.setAsync(`NEW_THUMBNAIL_EXPERIMENT:${newThumbnailAdditions[i].old_detail_id}:${newThumbnailAdditions[i].class}:${newThumbnailAdditions[i].question_id}`, JSON.stringify({ active }), "Ex", 60 * 60 * 24 * 2); // 2 days
                }
            }
        }
        job.progress(100);
        return {
            data: {
                done: true,
            },
        };
    } catch (err) {
        console.log(err);
        return { err };
    }
}
module.exports.start = start;
module.exports.opts = {
    cron: "0 * * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
