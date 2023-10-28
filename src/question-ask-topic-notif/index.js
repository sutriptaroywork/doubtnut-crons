/* eslint-disable no-await-in-loop */
const axios = require("axios");
const moment = require("moment");
const _ = require("lodash");
const {
    getMongoClient, config, kafka, mysql,
} = require("../../modules");

async function addRecord(client, studentId, expiry) {
    await client.collection("topic_notification").insertOne(
        { student_id: studentId, expireAt: expiry },
    );
}

async function getQuestionTopic(ocrText) {
    try {
        const options = {
            method: "POST",
            url: "http://preprocess.doubtnut.internal/api/v1/detect-ocr-topic",
            headers: {
                "Content-Type": "application/json",
            },
            timeout: 5000,
            data: {
                ocrText,
            },
        };
        const { data } = await axios(options);
        if (data.data.topic) {
            return data.data.topic[0];
        }
    } catch (e) {
        return "NONE";
    }
}

async function checkPreviousNotif(client, studentIds) {
    const notificationSent = await client.collection("topic_notification")
        .find({ student_id: { $in: studentIds } })
        .toArray();
    return notificationSent.map((item) => item.student_id);
}

async function sendNotification(client, studentDetails, expiry) {
    const notificationData = {
        event: "camera",
        message: "DOUBTNUT pe clear karo sare doubts 🤩",
        image: `${config.staticCDN}QNA_Thumbnail.webp`,
        firebase_eventtag: "topic_notification",
        data: {},
    };
    const notificationSentUsers = await checkPreviousNotif(client, studentDetails.map((item) => item.student_id));
    for (let i = 0; i < studentDetails.length; i++) {
        if (!_.includes(notificationSentUsers, studentDetails[i].student_id)) {
            const questionTopic = await getQuestionTopic(studentDetails[i].ocr_text.replace(/"/g, ""));
            if (questionTopic && questionTopic !== "NONE") {
                notificationData.title = `15min se Nahi pucha or #${questionTopic} ka sawal?`;
                await kafka.sendNotification([studentDetails[i].student_id], [studentDetails[i].gcm_reg_id], notificationData);
                addRecord(client, studentDetails[i].student_id, expiry);
            }
        }
    }
}

function getApplicableStudents(startTime, endTime, presentTime) {
    const sql = `SELECT q.student_id, q.ocr_text, s.gcm_reg_id FROM questions_new q
                 LEFT JOIN students s ON s.student_id = q.student_id
                 WHERE q.timestamp BETWEEN '${startTime}' AND '${endTime}' AND q.student_id
                 NOT IN ( SELECT student_id FROM questions_new WHERE timestamp BETWEEN '${endTime}' AND '${presentTime}')
                 AND q.ocr_text IS NOT NULL GROUP BY q.student_id HAVING COUNT(1) = 1`;
    return mysql.pool.query(sql)
        .then((res) => res[0]);
}

async function topicNotification(client, job) {
    const currDate = moment().add(5, "hours").add(30, "minutes");
    const presentTime = currDate.format("YYYY-MM-DD HH:mm::ss");
    const endTime = currDate.subtract(15, "minutes").format("YYYY-MM-DD HH:mm::ss");
    const startTime = currDate.subtract(15, "minutes").format("YYYY-MM-DD HH:mm::ss");
    const expiry = moment().add(5, "hours").add(30, "minutes").endOf("day")
        .format("YYYY-MM-DDTHH:mm:ss[Z]");

    const questionAskedStudents = await getApplicableStudents(startTime, endTime, presentTime);
    job.progress(30);
    await sendNotification(client, questionAskedStudents, expiry);
    job.progress(90);
    return true;
}

async function start(job) {
    job.progress(10);
    console.log("task started");
    const client = (await getMongoClient()).db("studentdb");
    await topicNotification(client, job);
    job.progress(100);
    console.log("task completed");
    return { data: "success" };
}

module.exports.start = start;
module.exports.opts = {
    cron: "*/15 * * * *", // Every 15 minutes
};
