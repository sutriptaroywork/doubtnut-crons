/* eslint-disable no-await-in-loop */
const axios = require("axios");
const moment = require("moment");
const _ = require("lodash");
const mysql = require("../../modules/mysql");
const { config } = require("../../modules");

const HOURS_INTERVAL = 3;

async function postMessage(data) {
    try {
        const microService = {
            method: "post",
            url: `${config.microUrl}api/study-group/post`,
            headers: {
                "x-auth-token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6NzI0NTE1LCJpYXQiOjE1OTY1MjIwNjgsImV4cCI6MTY1OTU5NDA2OH0.jCnoQt_VhGjC6EMq_ObPl9QpkBJNEAqQhPojLG_pz8c",
                "Content-Type": "application/json",
                Cookie: "__cfduid=d117dc0091ddb32cee1131365a76a7c931617628174",
            },
            data,
        };

        axios(microService)
            .then((response) => {
                console.log(JSON.stringify(response.data));
            })
            .catch((error) => {
                console.log(error);
            });
        return true;
    } catch (e) {
        return false;
    }
}

async function getFreeLiveClasses(datetime) {
    const sql = `SELECT distinct ls.*, sg.group_id,d.subject,d.chapter,d.master_chapter,lcr.resource_reference,lc.class,lc.locale,lc.course_exam,du.student_id, du.name
                FROM liveclass_stream ls
                         LEFT JOIN liveclass_course_details d on ls.detail_id = d.id
                         left join liveclass_course_resources lcr on lcr.liveclass_course_detail_id = d.id
                         left join liveclass_course lc on lc.id = d.liveclass_course_id
                         left join dashboard_users du ON ls.faculty_id = du.id
                         join study_group sg on sg.created_by=du.student_id
                where start_time >= '${datetime}'
                  and lcr.resource_type = 4
                  and lc.is_free = 1
                and sg.group_id like 'pgtf-%' and sg.is_active=1`;
    console.log(sql);
    const result = await mysql.pool.query(sql).then((res) => res[0]);
    return result;
}

async function structureResponse(resourceReference, topic, groupId, teacherId, teacherName) {
    const childWidget = {
        widget_data: {
            deeplink: `doubtnutapp://video?qid=${resourceReference}&page=STUDYGROUP`,
            question_text: topic,
            id: "question",
        },
        widget_type: "widget_asked_question",
    };
    const message = {
        widget_data: {
            child_widget: childWidget,
            created_at: moment().valueOf(),
            student_img_url: `${config.staticCDN}images/upload_45917205_1619087619.png`,
            title: `${teacherName} just started taking a live class`,
            sender_detail: "Sent by Doubtnut",
            visibility_message: "",
            widget_display_name: "Image",
            cta_text: "Join now",
            cta_color: "#ea532c",
            deeplink: `doubtnutapp://video?qid=${resourceReference}&page=STUDYGROUP`,
        },
        widget_type: "widget_study_group_parent",
    };

    return JSON.stringify({
        message,
        room_id: groupId,
        room_type: "public_groups",
        student_id: teacherId,
        attachment: "",
        attachment_mime_type: "",
        student_displayname: "Doubtnut",
        student_img_url: `${config.staticCDN}images/upload_45917205_1619087619.png`,
    });
}

async function start(job) {
    job.progress(10);
    const currDate = moment().add(5, "hours").add(30, "minutes");
    const liveClassFromTime = currDate.subtract(HOURS_INTERVAL, "hours").format("YYYY-MM-DD HH:mm:SS");
    job.progress(20);
    console.log(`fetching free live classes from ${liveClassFromTime}`);
    const freeLiveClasses = await getFreeLiveClasses(liveClassFromTime);
    console.log(`total free live classes found => ${freeLiveClasses.length}`);
    job.progress(50);
    const sentMessageGroups = [];
    for (let i = 0; i < freeLiveClasses.length; i++) {
        if (!_.includes(sentMessageGroups, freeLiveClasses[i].group_id)) {
            // process for sending free teacher group messages on micro
            const messageStructure = await structureResponse(freeLiveClasses[i].resource_reference, `${freeLiveClasses[i].chapter} (${freeLiveClasses[i].subject})`, freeLiveClasses[i].group_id, freeLiveClasses[i].student_id, freeLiveClasses[i].name);
            console.log(messageStructure, " message structure");
            await postMessage(messageStructure);
            await new Promise((resolve) => {
                console.log("Waiting for 100 ms....");
                setTimeout(resolve, 100);
            });
            sentMessageGroups.push(freeLiveClasses[i].group_id);
        }
    }
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "5 */3 * * *", // At 5 minutes past the hour, every 3 hours
};
