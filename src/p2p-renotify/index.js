/* eslint-disable no-await-in-loop */
const moment = require("moment");
const _ = require("lodash");
const { getMongoClient, kafka, mysql } = require("../../modules");

const TIME_RANGE = 15;
const minAllowedVersion = 906;
const collectionName = "p2p_active_members";
const db = "doubtnut";

async function getStudentDetails(students) {
    const sql = "SELECT student_id, gcm_reg_id FROM students WHERE student_id in (?)";
    const result = await mysql.pool.query(sql, [students])
        .then((res) => res[0]);
    console.log(result);
    return result;
}

async function notifyUsers(client, groupDetails) {
    try {
        const chunkSize = 200;
        const notificationData = {
            event: "doubt_pe_charcha",
            message: "Please help now",
            image: null,
            firebase_eventtag: "p2p_renotify",
        };

        for (const groupData of groupDetails) {
            let name = "Doubtnut User";
            let studentClass = 12;
            if (groupData.student_fname) {
                name = groupData.student_fname;
            }
            notificationData.title = `${name} needs your help with a doubt`;
            notificationData.data = {
                title: "Doubt", room_id: groupData.room_id, room_type: "p2p", is_host: false, is_reply: false, is_message: true,
            };
            if (groupData.question_image) {
                notificationData.image = groupData.question_image;
            }
            if (groupData.question_text) {
                notificationData.message = `Please help now on ${groupData.question_text}`;
            }
            if (groupData.student_class) {
                studentClass = parseInt(groupData.student_class);
            }
            console.log(notificationData);
            const randomStudentIds = await client.collection(collectionName).aggregate([
                {
                    $match: {
                        is_active: true,
                        student_class: studentClass,
                        version_code: { $gte: minAllowedVersion },
                    },
                },
                { $sample: { size: chunkSize } },
            ]).toArray();
            const studentIds = [];
            let students = null;
            if (!_.isEmpty(randomStudentIds)) {
                for (let s = 0; s <= randomStudentIds.length; s++) {
                    if (randomStudentIds[s] && randomStudentIds[s].student_id) {
                        studentIds.push(parseInt(randomStudentIds[s].student_id));
                    }
                }
                students = await getStudentDetails(studentIds);
            }
            if (students) {
                const notifReceivers = [];
                const gcmRegistrationId = [];
                for (let i = 0; i <= students.length; i++) {
                    if (students[i] && students[i].gcm_reg_id) {
                        notifReceivers.push(students[i].student_id);
                        gcmRegistrationId.push(students[i].gcm_reg_id);
                    }
                }
                await kafka.sendNotification(notifReceivers, gcmRegistrationId, notificationData);
            }
        }
        return true;
    } catch (e) {
        console.error(e);
        return false;
    }
}

function getNonHelperGroups() {
    const currDate = moment().add(5, "hours").add(30, "minutes");
    const minutes = currDate.minutes() % TIME_RANGE;
    const startTime = moment(currDate).subtract(TIME_RANGE + minutes, "minutes").format("YYYY-MM-DD HH:mm:ss");
    const endTime = moment(currDate).subtract(minutes, "minutes").format("YYYY-MM-DD HH:mm:ss");

    const sql = `SELECT d.*, s.student_fname, s.student_class FROM (SELECT student_id, room_id, question_id FROM doubt_pe_charcha
                 WHERE room_id IN (SELECT r.room_id FROM (SELECT room_id, count(1) as total_members FROM doubt_pe_charcha
                 WHERE created_at BETWEEN '${startTime}' AND '${endTime}' GROUP BY room_id HAVING total_members = 1
                 ORDER BY total_members) AS r) AND is_host = 1) AS d INNER JOIN students s on s.student_id = d.student_id WHERE s.is_online >= ?`;

    console.log(startTime, endTime);
    return mysql.pool.query(sql, [minAllowedVersion]).then((res) => res[0]);
}

function getQuestionDetails(questions) {
    const sql = "SELECT question_id, question_image, ocr_text as question_text FROM questions_new WHERE question_id in (?)";
    return mysql.pool.query(sql, [questions])
        .then((res) => res[0]);
}

async function start(job) {
    const client = (await getMongoClient()).db(db);
    const nonHelperGroups = await getNonHelperGroups();
    console.log("fetched questions asked students ", nonHelperGroups.length);
    job.progress(10);
    let i; let j; let temporary; const chunk = 10000;
    for (i = 0, j = nonHelperGroups.length; i < j; i += chunk) {
        temporary = nonHelperGroups.slice(i, i + chunk);
        const questionIds = [];
        for (let k = 0; k < temporary.length; k++) {
            if (temporary[k] && temporary[k].question_id) {
                questionIds.push(temporary[k].question_id);
            }
        }
        console.log("questionIds ", questionIds.length);
        if (!_.isEmpty(questionIds)) {
            const questionDetails = await getQuestionDetails(questionIds);
            console.log("fetched student details ", questionDetails.length);
            const mergedList = questionDetails.map((questionData) => ({ ...questionData, ...temporary.find((groupDetails) => groupDetails.question_id === questionData.question_id) }));
            notifyUsers(client, mergedList);
        }
    }
    job.progress(100);
    return 1;
}

module.exports.start = start;
module.exports.opts = {
    cron: "5-59/15 * * * *", // Every 15 minutes, minutes 5 through 59 past the hour
};
