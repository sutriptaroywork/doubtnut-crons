/* eslint-disable no-await-in-loop */
const axios = require("axios");
const moment = require("moment");
const _ = require("lodash");
const qs = require("qs");
const mysql = require("../../modules/mysql");
const { config } = require("../../modules");

const TIME_RANGE = 10;
const TOTAL_MEMBERS_TO_ENABLE_GROUP = 2;

async function postMessage(messages) {
    const microService = {
        method: "POST",
        url: `${config.microUrl}api/study-group/bulk-posts`,
        timeout: 5000,
        headers: {
            "x-auth-token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6NzI0NTE1LCJpYXQiOjE1OTY1MjIwNjgsImV4cCI6MTY1OTU5NDA2OH0.jCnoQt_VhGjC6EMq_ObPl9QpkBJNEAqQhPojLG_pz8c",
            "Content-Type": "application/x-www-form-urlencoded",
            Cookie: "__cfduid=d117dc0091ddb32cee1131365a76a7c931617628174",
        },
        data: qs.stringify({ messages }),
    };

    axios(microService)
        .then((response) => {
            console.log(JSON.stringify(response.data));
        })
        .catch((error) => {
            console.log(error);
        });
}

async function getActiveGroups(roomIds) {
    const sql = `SELECT DISTINCT a.group_id FROM (SELECT sg.group_id, COUNT(DISTINCT sgm.student_id) AS members FROM study_group sg
                 JOIN study_group_members sgm ON sgm.study_group_id = sg.id AND sgm.is_active = 1
                 WHERE sg.group_id IN (?) GROUP BY sg.group_id) as a
                 LEFT JOIN study_group_reporting sgr ON sgr.student_id IS NULL AND sgr.study_group_id = a.group_id
                 WHERE (sgr.id IS NULL OR sgr.status = 2) AND a.members >= ?`;
    const result = await mysql.pool.query(sql, [roomIds, TOTAL_MEMBERS_TO_ENABLE_GROUP]).then((res) => res[0]);
    return result.map((item) => item.group_id);
}

async function structureResponse(student, currentTime) {
    let sendMsg = false;
    let widgetData;

    if (student.question_image) {
        const mimeType = ["png", "jpg", "webp", "jpeg"];
        const attachmentMimeType = student.question_image.split(".")[1];
        if (mimeType.includes(attachmentMimeType)) {
            widgetData = {
                deeplink: `doubtnutapp://full_screen_image?ask_que_uri=${config.staticCloudfrontCDN}images/${student.question_image}&title=study_group`,
                question_image: `${config.staticCloudfrontCDN}images/${student.question_image}`,
            };
            sendMsg = true;
        }
    }

    if (!sendMsg && student.ocr_text) {
        widgetData = {
            deeplink: null,
            question_text: `${student.ocr_text}`,
        };
        sendMsg = true;
    }

    if (sendMsg) {
        widgetData.id = "question";

        const childWidget = {
            widget_data: widgetData,
            widget_type: "widget_asked_question",
        };
        const message = {
            widget_data: {
                child_widget: childWidget,
                created_at: currentTime.valueOf(),
                student_img_url: `${config.staticCloudfrontCDN}images/upload_45917205_1619087619.png`,
                title: `${student.name} asked this question! Can you help ${student.name}?`,
                sender_detail: "Sent by Doubtnut",
                visibility_message: "",
                widget_display_name: "Image",
            },
            widget_type: "widget_study_group_parent",
        };

        return {
            message,
            room_id: student.room_id,
            room_type: "study_group",
            student_id: parseInt(student.student_id),
            attachment: "",
            attachment_mime_type: "",
            student_displayname: student.name,
            student_img_url: "",
            created_at: currentTime.toISOString(),
            updated_at: currentTime.toISOString(),
            is_active: true,
            is_deleted: false,
        };
    }
    return false;
}

async function questionAskedMessage(studentDetails) {
    const currentTime = moment().add(5, "hours").add(30, "minutes");
    const questionAskedStudents = new Set();
    const roomIdList = new Set();

    studentDetails.forEach((item) => {
        roomIdList.add(item.room_id);
    });
    const messages = [];
    const activeGroups = await getActiveGroups([...roomIdList]);
    if (!_.isEmpty(activeGroups)) {
        for (const student of studentDetails) {
            if (_.includes(activeGroups, student.room_id) && questionAskedStudents.size !== questionAskedStudents.add(JSON.stringify({ student_id: student.student_id, room_id: student.room_id })).size) {
                const response = await structureResponse(student, currentTime);
                if (response) {
                    messages.push(response);
                }
            }
        }
    }

    const chunk = 1000;
    for (let i = 0, j = messages.length; i < j; i += chunk) {
        postMessage(messages.slice(i, i + chunk));
        await new Promise((resolve) => {
            console.log("Waiting for 200 ms....");
            setTimeout(resolve, 200);
        });
    }
}

function getQuestionAskedStudents(startTime, presentTime, offset) {
    // this query is getting a list of all the students who have asked question in past 30 minutes but have not viewed
    // any answer video(i.e student_id for that question_id wouldn't be available in vvs)
    const sql = `SELECT q.student_id, q.question_id, q.question_image, q.ocr_text, q.timestamp FROM questions_new q WHERE 
                 NOT EXISTS (SELECT v.question_id FROM video_view_stats v WHERE v.student_id = q.student_id AND v.created_at 
                 BETWEEN '${startTime}' AND '${presentTime}' AND view_from = 'SRP') AND q.timestamp BETWEEN '${startTime}' AND '${presentTime}' LIMIT 100 OFFSET ${offset}`;
    console.log(sql, ' sql');
    return mysql.pool.query(sql).then((res) => res[0]);
}

function getStudyGroupUsersOnly(studentIdList) {
    // This query will get student_name, group_id, student_id which are active in any study group
    const sql = `SELECT IFNULL(s.student_fname, 'Doubtnut User') AS name, sg.group_id AS room_id, g.student_id FROM
                 study_group_members g JOIN study_group sg on sg.id = g.study_group_id AND sg.is_active = 1
                 JOIN students s ON s.student_id = g.student_id LEFT JOIN study_group_reporting sgr 
                 ON sgr.student_id = g.student_id AND sgr.study_group_id = sg.group_id
                 WHERE g.student_id IN (?) AND g.is_active = 1 AND (sgr.id IS NULL OR sgr.status = 2)`;
    return mysql.pool.query(sql, [studentIdList])
        .then((res) => res[0]);
}

async function start(job) {
    job.progress(10);
    const currDate = moment().add(5, "hours").add(30, "minutes").toDate();
    const minutes = currDate.getMinutes() % TIME_RANGE;
    const presentTime = moment(currDate).subtract(minutes, "minutes").toISOString().slice(0, 19)
        .replace("T", " ");
    const startTime = moment(currDate).subtract(TIME_RANGE + minutes, "minutes").toISOString().slice(0, 19)
        .replace("T", " ");

    let questionAskedStudents = [];
    const fetchChunk = 200;
    for (let i = 0; i < 10000; i += fetchChunk) {
        console.time('starting');
        const data = await getQuestionAskedStudents(startTime, presentTime, i);
        console.timeEnd('starting');
        questionAskedStudents = questionAskedStudents.concat(data);
        if (data.length < fetchChunk) {
            console.log("No more data");
            break;
        }
    }
    job.progress(20);
    questionAskedStudents = _.orderBy(questionAskedStudents, ["timestamp"], ["desc"]);
    const distinctStudentIds = new Set();
    const distinctUsers = [];
    for (let i = 0; i < questionAskedStudents.length; i++) {
        if (distinctStudentIds.size !== distinctStudentIds.add(questionAskedStudents[i].student_id).size) {
            distinctUsers.push(questionAskedStudents[i]);
        }
    }
    job.progress(30);
    const chunk = 10000;
    for (let i = 0, j = distinctUsers.length; i < j; i += chunk) {
        const temporary = distinctUsers.slice(i, i + chunk);
        const studentIds = [];
        for (let k = 0; k < temporary.length; k++) {
            if (!_.isEmpty(temporary[k]) && temporary[k].student_id) {
                studentIds.push(temporary[k].student_id);
            }
        }
        if (studentIds.length) {
            const studyGroupUsers = await getStudyGroupUsersOnly(studentIds);
            const mergedList = studyGroupUsers.map((t1) => ({ ...t1, ...temporary.find((t2) => t2.student_id === t1.student_id) }));
            if (mergedList.length) {
                await questionAskedMessage(mergedList);
            }
        }
    }
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "5 10 * * *", // Every 30 minutes, minutes 5 through 59 past the hour, at 12:00 AM through 06:59 PM and 09:00 PM through 11:59 PM
    // cron: "5-59/30 0-18,21-23 * * *", // Every 30 minutes, minutes 5 through 59 past the hour, at 12:00 AM through 06:59 PM and 09:00 PM through 11:59 PM
};
