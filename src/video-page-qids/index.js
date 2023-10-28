/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const { redis } = require("../../modules/index");
const { mysql } = require("../../modules");

async function getActiveQids() { // 80 ms
    const mysqlQ = "select * from video_page_qids where is_active = 1";
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}

async function setQidCarousel(classMeta, locale, data) {
    return redis.setAsync(`videopage:qid:${classMeta}:${locale}`, JSON.stringify(data), "Ex", 60 * 60 * 24);
}

async function setQidData(id, data) {
    return redis.setAsync(`videopage:table:${id}`, JSON.stringify(data), "Ex", 60 * 60 * 24);
}

async function start(job) {
    try {
        const allActiveQids = await getActiveQids();
        const groupByClassQids = _.groupBy(allActiveQids, "class");
        for (const classMeta in groupByClassQids) {
            if ({}.hasOwnProperty.call(groupByClassQids, classMeta)) {
                const classQids = groupByClassQids[classMeta];
                const qroupByLocale = _.groupBy(classQids, "locale");
                for (const locale in qroupByLocale) {
                    if ({}.hasOwnProperty.call(qroupByLocale, locale)) {
                        const ids = qroupByLocale[locale];
                        await setQidCarousel(classMeta, locale, ids);
                    }
                }
            }
        }
        for (let i = 0; i < allActiveQids.length; i++) {
            if (allActiveQids[i].query !== "" && allActiveQids[i].query !== null) {
                if (allActiveQids[i].query[allActiveQids[i].query.length - 1] === ";") {
                    allActiveQids[i].query = allActiveQids[i].query.slice(0, allActiveQids[i].query.length - 1);
                }
                const query = `${allActiveQids[i].query} limit ${allActiveQids[i].query_data_limit}`;
                const data = await mysql.pool.query(query).then(([res]) => res);
                if (data && data.length > 0) {
                    await setQidData(allActiveQids[i].id, data);
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
        return {
            err,
        };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 2 * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
