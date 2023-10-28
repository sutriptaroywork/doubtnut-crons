/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const { redis } = require("../../modules/index");
const { mysql } = require("../../modules");

async function getActiveCarousels() { // 90 ms
    const mysqlQ = "select * from video_page_carousels where is_active = 1";
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}

async function setCaouselData(carouselId, data) {
    return redis.setAsync(`videopage:carouseldata:${carouselId}`, JSON.stringify(data), "Ex", 60 * 60 * 4);
}

async function setActiveCarousel(classMeta, data) {
    return redis.setAsync(`videopage:carousel:class:${classMeta}`, JSON.stringify(data), "Ex", 60 * 60 * 4);
}

async function start(job) {
    try {
        const allActiveCarousels = await getActiveCarousels();
        for (let i = 0; i < allActiveCarousels.length; i++) {
            if (allActiveCarousels[i].query !== "" && allActiveCarousels[i].query != null) {
                if (allActiveCarousels[i].query[allActiveCarousels[i].query.length - 1] === ";") {
                    allActiveCarousels[i].query = allActiveCarousels[i].query.slice(0, allActiveCarousels[i].query.length - 1);
                }
                const query = `${allActiveCarousels[i].query} limit ${allActiveCarousels[i].query_data_limit}`;
                const data = await mysql.pool.query(query).then(([res]) => res);
                if (data && data.length > 0) {
                    await setCaouselData(allActiveCarousels[i].id, data);
                }
            }
        }
        const groupByClass = _.groupBy(allActiveCarousels, "class");
        for (const classMeta in groupByClass) {
            if ({}.hasOwnProperty.call(groupByClass, classMeta)) {
                const classCarousels = groupByClass[classMeta];
                await setActiveCarousel(classMeta, classCarousels);
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
        return {
            err,
        };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 */2 * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
