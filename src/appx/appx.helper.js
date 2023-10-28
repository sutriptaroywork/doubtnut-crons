const moment = require("moment");
const _ = require("lodash");

async function insertIgnoreUpcomingClasss(db, upcoming) {
    const sql = "INSERT IGNORE into appx_sync set ?";
    return db.query(sql, [upcoming]);
}
async function insertIntoAppXSync(db, appxSyncData) {
    const sql = "INSERT IGNORE into appx_sync set ?";
    return db.query(sql, [appxSyncData]);
}
async function addQuestion(db, questionMeta) {
    const sql = "INSERT INTO questions SET ?";
    return db.query(sql, [questionMeta]);
}

async function tranformAppxToDN(appx, db) {
    const subjectArray = appx.subject.toLowerCase().split(" by ");

    const subject = _.capitalize(subjectArray[0].trim());

    /* Check If Appx Teacher Exist */
    const teacherData = await getAppxTeacher(db, appx.faculty_name, appx.liveclass_course_id);
    if (teacherData.length > 0) {
        appx.faculty_id = teacherData[0].id;
    } else {
        /* If not Create One */
        const insertTeacherData = await insertAppxTeacher(db, appx);
        appx.faculty_id = insertTeacherData.insertId;
    }

    const lectureCount = await getPreviousClassesOfUpcomingClass(db, { faculty_id: appx.faculty_id, liveclass_course_id: appx.liveclass_course_id, master_chapter: appx.topic });
    const lectureID = ` - L${lectureCount[0].total + 1}`;

    const castData = {};
    castData.liveclass_course_id = appx.liveclass_course_id;
    castData.subject = subject;
    castData.chapter = appx.topic + lectureID;
    castData.class = 14;
    castData.live_at = moment(parseInt((appx.strtotime + 19800) * 1000)).format();
    // castData.live_at = moment().format();

    castData.is_free = 0;
    castData.master_chapter = appx.topic;
    castData.course_type = "PAID";
    castData.faculty_id = appx.faculty_id;
    castData.description = appx.description;
    castData.faculty_name = appx.faculty_name;
    castData.faculty_image = appx.faculty_image;
    return castData;
}

function createQuestionsMetaFromLive(liveclass, resource_type) {
    const vodSid = -159;
    const doubt = `AppX_${liveclass.class}_${liveclass.subject.substr(0, 3)}_${liveclass.faculty_id}_${moment.parseZone(liveclass.live_at).format("YYYYMMDD")}_${moment.parseZone(liveclass.live_at).format("HH")}`;
    const question = {};
    question.student_id = vodSid;
    question.class = liveclass.class;
    question.subject = liveclass.subject;
    question.question = liveclass.chapter;
    question.ocr_text = liveclass.chapter;
    question.is_answered = resource_type == 1 ? 1 : 0;
    question.doubt = doubt;
    return question;
}

async function addAnswer(db, answerMeta) {
    const sql = "INSERT INTO answers SET ?";
    return db.query(sql, answerMeta);
}

async function insertUpcomingClassIntoCourseDetails(db, data) {
    const sql = "insert into liveclass_course_details SET liveclass_course_id=?, subject=?, chapter=?,class=?,live_at=?,is_free=?,master_chapter=?,course_type=?,faculty_id=?";
    return db.query(sql, [data.liveclass_course_id, data.subject, data.master_chapter, data.class, data.live_at, data.is_free, data.master_chapter, data.course_type, data.faculty_id]);
}

async function insertFacultyMapping(db, detail_id, faculty_id) {
    const sql = "insert into liveclass_detail_faculty_mapping SET detail_id = ?, faculty_id = ? ";
    return db.query(sql, [detail_id, faculty_id]);
}
async function insertStreamMapping(db, detail_id, faculty_id) {
    const sql = "insert into liveclass_stream SET detail_id = ?, faculty_id = ?, is_active=0";
    return db.query(sql, [detail_id, faculty_id]);
}

async function insertUpcomingClassIntoCourseResources(db, detailID, course, questionId, resource_type = 4, metaInfo = null) {
    const sql = "insert into liveclass_course_resources SET liveclass_course_id=?,liveclass_course_detail_id=?, subject=?, topic=?,expert_name=?,expert_image=?,q_order=?,resource_type=?,resource_reference = ?,class=?,player_type=?,is_processed=?,is_resource_created=?,meta_info = ?";
    return db.query(sql, [course.liveclass_course_id, detailID, course.subject, course.chapter, course.faculty_name, course.faculty_image, 1, resource_type, questionId, course.class, "liveclass", 0, 0, metaInfo]);
}

async function updateAppxSync(db, updateObj, syncId) {
    console.info(updateObj, "appx sync update", syncId, "SYNC ID");
    const sql = "update appx_sync  set ? where sync_id = ?";
    return db.query(sql, [updateObj, syncId]);
}

async function getCourseResourceByResourceReference(db, resourceReference) {
    const sql = "select * from course_resources where resource_reference = ? and resource_type IN (1,4)";
    return db.query(sql, [resourceReference]);
}

async function getAppxSyncDataById(db, appXId) {
    const sql = "select * from appx_sync where id = ?";
    return db.query(sql, [appXId]);
}
async function getByQuestionId(db, questionID) {
    const sql = "SELECT a.*, answers.* FROM (Select * from questions where question_id=?) as a inner join answers on a.question_id = answers.question_id order by answers.answer_id desc limit 1";
    return db.query(sql, [questionID]);
}
async function addAnswerVideoResource(db, data) {
    const sql = "insert into answer_video_resources set ?";
    return db.query(sql, [data]);
}
async function updateAnswerVideoResource(db, data, answerId) {
    const sql = "update answer_video_resources set ? where answer_id = ?";
    return db.query(sql, [data, answerId]);
}
async function updateStreamStartTime(db, resourceID) {
    const sql = `update course_resources set stream_status='ACTIVE' where id=${resourceID}`;
    return db.query(sql);
}
async function getAppxTeacher(db, teacherName, course) {
    const sql = "select * from dashboard_users where name = ? and course = ? and type = 'AppX' ";
    return db.query(sql, [teacherName, course]);
}
async function insertAppxTeacher(db, appx) {
    const teacherData = {
        name: appx.faculty_name,
        image_url: appx.faculty_image,
        course: appx.liveclass_course_id,
        image_bg_liveclass: appx.faculty_image,
        type: "AppX",

    };
    const sql = "insert into dashboard_users set ?";
    return db.query(sql, [teacherData]);
}
async function getPreviousClassesOfUpcomingClass(db, details) {
    const sql = "SELECT count(1) as total FROM liveclass_course_details where faculty_id = ? and liveclass_course_id = ? and master_chapter = ? and is_replay = 0";

    return db.query(sql, [details.faculty_id, details.liveclass_course_id, details.master_chapter]);
}

async function getListCurrentLiveClassByAppxCourseId(db, appXCourseId) {
    const sql = "select id,question_id,appx_course_id from appx_sync where status = 1 and appx_course_id = ?";
    return db.query(sql, appXCourseId);
}

module.exports = {
    insertIgnoreUpcomingClasss, insertIntoAppXSync, tranformAppxToDN, addQuestion, createQuestionsMetaFromLive, addAnswer, insertUpcomingClassIntoCourseDetails, insertStreamMapping, insertUpcomingClassIntoCourseResources, updateAppxSync, insertFacultyMapping, getAppxSyncDataById, getCourseResourceByResourceReference, getByQuestionId, addAnswerVideoResource, updateStreamStartTime, updateAnswerVideoResource, getPreviousClassesOfUpcomingClass, getListCurrentLiveClassByAppxCourseId,

};
