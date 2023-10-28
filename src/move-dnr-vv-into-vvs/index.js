const moment = require("moment");
const _ = require("lodash");
const redisClient = require("../../modules/redis");
const { mysql, getMongoClient } = require("../../modules");

function addDnrViewsIntoVvs(object) {
    const sql = "INSERT INTO video_view_stats SET ?";
    return mysql.writePool.query(sql, object);
}

async function start(job) {
    job.progress(10);
    const client = (await getMongoClient()).db('doubtnut');
    job.progress(30);

    const yesterdayStart = moment().subtract(1, 'day').startOf('d').add(5, 'hours').add(30, 'minutes').toISOString();
    const yesterdayEnd = moment().subtract(1, 'day').endOf('d').add(5, 'hours').add(30, 'minutes').toISOString();
    const dnrViews = await client.collection('dnr_vv').find({ created_at: { $gte: yesterdayStart, $lte: yesterdayEnd } }).toArray();

    if (dnrViews.length > 0) {
        for(let i = 0; i < dnrViews.length; i++) {
            const viewsData = dnrViews[i];
            await addDnrViewsIntoVvs({
                student_id: viewsData.student_id,
                question_id: viewsData.question_id,
                answer_id: viewsData.answer_id,
                answer_video: viewsData.answer_video,
                video_time: viewsData.video_time,
                engage_time: viewsData.video_time,
                created_at: viewsData.created_at,
                ip_address: viewsData.ip_address,
                source: viewsData.source,
                view_from: viewsData.view_from,
            });
        }
    }

    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "15 3 * * *", // run at 3:15 everyday
};
