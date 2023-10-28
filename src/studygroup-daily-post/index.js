const moment = require("moment");
const _ = require("lodash");

const { mysql } = require("../../modules");
const Utility = require("../../modules/study_group_utility");
const StaticData = require("./data");

async function getquestionOfTheHour(groupId, hour) {
    /*** Getting Question of the hour ***/
    const sql = `SELECT sgd.*, qn.question_image, qn.ocr_text FROM study_group_p2p_doubt_request sgd LEFT JOIN questions_new qn ON sgd.question_id = qn.question_id WHERE sgd.study_group_id = ? AND sgd.scheduled_date >= CONCAT(CURDATE(), ' 00:00:00') AND sgd.scheduled_date <= CONCAT(CURDATE(), ' 23:59:59') AND sgd.hour = ? AND sgd.is_active = 1`;
    return mysql.pool.query(sql, [groupId, hour]).then((res) => res[0]);
}

async function start(job) {
    job.progress(10);
    console.log("task started");

    // initializing varibales
    const { groupIds, nameList, question } = StaticData;
    // const todaysDate = moment().utcOffset("+05:30").format("YYYY-MM-DD");
    const currentTime = moment().utcOffset("+05:30");
    const hour = moment().utcOffset("+05:30").hour();
    const dataList = [];
    
    for(let i = 0; i < groupIds.length; i++) {
        const qDetailsForThisHour = await getquestionOfTheHour(groupIds[i], hour);

        if (qDetailsForThisHour.length > 0) {
            // making db object
            const student = {
                room_id: groupIds[i],
                ocr_text: qDetailsForThisHour[0].ocr_text,
                question_image: qDetailsForThisHour[0].question_image,
                student_id: 98,
                name: _.sample(nameList),
                title: question,
            };
            let obj = Utility.structureResponse(student, currentTime, "image");
            if (obj) {
                // inserting into db
                await Utility.postMessage(obj, [groupIds[i]]);
            }
            obj = Utility.structureResponse(student, currentTime, "text");
            if (obj) {
                // inserting into db
                await Utility.postMessage(obj, [groupIds[i]]);
            }
        }
    }

    job.progress(100);
    console.log("task completed");
    return { data: "success" };
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 8-23 * * *", // On every hour between 8 AM to 11 PM
};
