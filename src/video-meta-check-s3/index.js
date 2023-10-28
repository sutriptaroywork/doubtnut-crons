/* eslint-disable no-await-in-loop */
const AWS = require("aws-sdk");
const { mysql } = require("../../modules");

const BUCKET = "doubtnutteststreamin-hosting-mobilehub-1961518253";
const s3 = new AWS.S3();

function getTotalSize() {
    const sql = "SELECT count(*) as size FROM answers WHERE answer_video like '%.mp4' and updated_at >= (NOW() - INTERVAL 1 DAY)";
    return mysql.pool.query(sql).then(([res]) => res);
}

function getVideoList(page) {
    const sql = `select answer_id, answer_video from answers WHERE answer_video like '%.mp4' and updated_at >= (NOW() - INTERVAL 1 DAY) order by answer_id asc limit 50000 offset ${page * 50000}`;
    // console.log(sql);
    return mysql.pool.query(sql).then(([res]) => res);
}

function updateVideoSize(id, key) {
    const obj = { filesize_bytes: key };
    const sql = `update IGNORE answers set ? where answer_id=${id}`;
    return mysql.writePool.query(sql, obj).then(([res]) => res);
}

const putObjectWrapper = (params) => new Promise((resolve) => {
    s3.headObject(params, (err, result) => {
        if (err) resolve(err);
        if (result) resolve(result);
    });
});

function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

async function updateVideoMetaInfo() {
    const videoSize = await getTotalSize();
    const totalSize = Math.floor(videoSize[0].size / 50000) + 1;
    console.log("totalSize", totalSize);
    for (let j = 0; j < totalSize; j++) {
        const data = await getVideoList(j);
        for (let i = 0; i < data.length; i++) {
            // console.log(data[i]);
            const key = data[i].answer_video;
            const params = { Bucket: BUCKET, Key: key };
            const videoData = await putObjectWrapper(params);
            if (videoData && videoData.ContentLength) {
                console.log(`${j}th iteration ${i} element value is ${data[i].answer_id}`);
                // console.log(videoData);
                updateVideoSize(data[i].answer_id, videoData.ContentLength);
            }
            if (i % 3000 === 0) {
                await sleep(100);
            }
        }
    }
}

async function start(job) {
    await updateVideoMetaInfo();
    await job.progress(100);
    console.log(`the script successfully ran at ${new Date()}`);
    return { data: "success" };
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 1 * * *",
};
