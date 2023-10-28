/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const moment = require("moment");
const { mysql } = require("../../modules/index");
const config = require("../../modules/config");
const tencentcloud = require("../../node_modules/tencentcloud-sdk-nodejs-intl-en");
const Tencent = require("./tencent");

async function getData(date) {
    const sql = "select b.*,c.*, c.id as answer_video_resources_id from (select * from course_resources where resource_type=1) as a left join (select answer_id as answer_id, question_id from answers) as b on a.resource_reference=b.question_id left join answer_video_resources as c on b.answer_id=c.answer_id where c.answer_id is not null and c.resource_order <> 1 and c.is_active=1 and c.resource like '%myqcloud%' and c.created_at < ? order by c.created_at desc";
    return mysql.pool.query(sql, [date]).then((res) => res[0]);
}

async function deleteAnswerVideoResources(answerVideoResoucesId) {
    const sql = `delete from answer_video_resources where id = ${answerVideoResoucesId}`;
    return mysql.writePool.query(sql).then((res) => res[0]);
}

async function deleteMedia(fileId) {
    return new Promise((resolve) => {
        const secretID = config.tencent_secret_id;
        const secretKey = config.tencent_secret_key;
        const { Credential } = tencentcloud.common;
        const cred = new Credential(secretID, secretKey);
        const VodClient = tencentcloud.vod.v20180717.Client;
        const client = new VodClient(cred, "ap-mumbai");
        const req1 = new Tencent.DeleteMediaRequest({ FileId: fileId });
        client.DeleteMedia(req1, (err, response) => {
            console.log(err);
            if (err) {
                console.log(err);
                resolve({ error: "errored" });
            }
            resolve(response);
        });
    });
}

async function getFiles(resourceID) {
    return new Promise((resolve) => {
        const secretID = config.tencent_secret_id;
        const secretKey = config.tencent_secret_key;
        const VodClient = tencentcloud.vod.v20180717.Client;
        const { Credential } = tencentcloud.common;
        const cred = new Credential(secretID, secretKey);
        const client = new VodClient(cred, "ap-mumbai");
        const req1 = new Tencent.SearchMediaRequest({ StreamId: resourceID });
        client.SearchMedia(req1, (err, response) => {
            if (err) {
                resolve(null);
            }
            resolve(response);
        });
    });
}

async function start(job) {
    try {
        job.progress(0);
        // const db = {};
        // db.mysql = {};
        // mysqlClient = new Database(config.mysql_analytics);
        // mysqlWriteClient = new Database(config.mysql_write);
        // db.mysql.read = mysqlClient;
        // db.mysql.write = mysqlWriteClient;
        const date = moment().add(5, "hours").add(30, "minutes").subtract(7, "days")
            .format("YYYY-MM-DD");
        const filesData = await getData(date);
        console.log(filesData.length);
        for (let i = 0; i < filesData.length; i++) {
            console.log(filesData[i]);
            const response = await getFiles(filesData[i].question_id.toString());
            // console.log('response')
            // console.log(response)
            if (_.isNull(response)) {
                console.log("no response");
                console.log(filesData[i].question_id.toString());
            } else {
                for (let j = 0; j < response.MediaInfoSet.length; j++) {
                    console.log("response.MediaInfoSet[j]");
                    console.log(response.MediaInfoSet[j]);
                    const deleteResponse = await deleteMedia(response.MediaInfoSet[j].FileId);
                    console.log(deleteResponse);
                    await deleteAnswerVideoResources(filesData[i].answer_video_resources_id);
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
    cron: "30 20 * * *", // * 2:00 at night everyday
};
