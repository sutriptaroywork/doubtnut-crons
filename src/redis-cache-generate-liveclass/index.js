/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const moment = require("moment");
const { mysql, redis } = require("../../modules/index");
const helperRecursive = require("./helper");

const redisKey = {
    upcoming: { key: "{COURSE_UPCOMING}:", expiry: 60 * 60, sortKey: [["live_at"], ["asc"]] },
    live_now: { key: "{COURSE_LIVE}:", expiry: 60 * 60 },
    replay: { key: "{COURSE_REPLAY}:", expiry: 60 * 60 },
    recent_boards: { key: "{COURSE_BOARDS}:", expiry: 60 * 60, sortKey: [["live_at"], ["desc"]] },
    recent_iit_neet: { key: "{COURSE_IIT_NEET}:", expiry: 60 * 60, sortKey: [["live_at"], ["desc"]] },
    course_live_section_home: { key: "course_live_section_home", expiry: 60 * 60 },
    course_live_section: { key: "course_live_section", expiry: 60 * 60 },
    revision_classes: { key: "revision_class:", expiry: 60 * 60 },
};

async function getCourseAssortmentClassCombination() {
    const sql = "select distinct assortment_id, class, is_free from course_details where assortment_type='course' and start_date < now() and end_date > now()";
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getUpcomingLecturesByAssortmentId(assortmentId) {
    const sql = "select b.*,a.live_at,d.*,e.course_resource_id as chapter_assortment from (select * from course_resource_mapping where resource_type='resource' and is_replay=0 and live_at > now()) as a inner join (select * from course_resources cr where resource_type in (1,4,8)) as b on a.course_resource_id=b.id inner join (select assortment_id,is_free,meta_info as assortment_locale from course_details where is_free=1 and assortment_type='resource_video') as d on d.assortment_id=a.assortment_id inner join (select course_resource_id,assortment_id from course_resource_mapping where resource_type='assortment') as c on c.course_resource_id=a.assortment_id inner join (select course_resource_id,assortment_id from course_resource_mapping where resource_type='assortment') as e on e.course_resource_id=c.assortment_id inner join (select course_resource_id,assortment_id from course_resource_mapping where assortment_id = ?) as f on f.course_resource_id=e.assortment_id group by b.resource_reference order by a.live_at limit 10";
    return mysql.pool.query(sql, [assortmentId]).then((res) => res[0]);
}

async function getLiveNowLecturesByAssortmentId(assortmentId) {
    const sql = "select a.*,b.live_at,d.*,e.answer_id,e.duration, case when du.image_url_left_full is null then du.image_url ELSE du.image_url_left_full end as expert_image2 from (select *,case when player_type='youtube' and meta_info is not null then meta_info ELSE resource_reference end as question_id from course_resources cr where resource_type in (1,4,8)) as a inner join (select * from course_resource_mapping where resource_type='resource' and live_at>=CURRENT_DATE() and is_replay=0 and live_at < now()) as b on b.course_resource_id=a.id inner join (select assortment_id,is_free,meta_info,category from course_details where assortment_type='resource_video') as d on d.assortment_id=b.assortment_id inner join (select course_resource_id,assortment_id from course_resource_mapping where resource_type='assortment') as c on c.course_resource_id=b.assortment_id and c.assortment_id in (select course_resource_id from course_resource_mapping where assortment_id in (select course_resource_id from course_resource_mapping where assortment_id = ?)) left join answers as e on e.question_id=a.question_id left join dashboard_users as du on a.faculty_id=du.id where (a.stream_status='ACTIVE') OR (DATE_ADD(b.live_at,INTERVAL e.duration SECOND) > now()) group by a.question_id";
    return mysql.pool.query(sql, [assortmentId]).then((res) => res[0]);
}

async function getReplayLecturesByAssortmentId(assortmentId) {
    const sql = "select a.*,b.live_at,d.*,e.answer_id,e.duration from (select *,case when player_type='youtube' and meta_info is not null then meta_info ELSE resource_reference end as question_id from course_resources cr where resource_type in (1,4,8)) as a inner join (select * from course_resource_mapping where resource_type='resource' and live_at>=CURRENT_DATE() and live_at<now() and is_replay=2) as b on b.course_resource_id=a.id inner join (select assortment_id,is_free,meta_info as course_language from course_details where assortment_type='resource_video' and is_free=1) as d on d.assortment_id=b.assortment_id inner join (select course_resource_id,assortment_id from course_resource_mapping where resource_type='assortment') as c on c.course_resource_id=b.assortment_id and c.assortment_id in (select course_resource_id from course_resource_mapping where assortment_id in (select course_resource_id from course_resource_mapping where assortment_id = ?)) left join answers as e on e.question_id=a.question_id group by a.question_id limit 3";
    return mysql.pool.query(sql, [assortmentId]).then((res) => res[0]);
}

async function getRecentLecturesByAssortmentId(assortmentId, startTime, endTime, category, studentClass) {
    let sql = "";
    if (parseInt(studentClass) === 14) {
        sql = "SELECT cr.*, a.*, crm4.course_resource_id,crm4.name,crm4.resource_type,crm4.live_at, crm3.assortment_id as chapter_assortment from (SELECT DISTINCT assortment_id, display_name,is_free,meta_info as assortment_locale,category_type as course_session FROM course_details WHERE assortment_id = ? and parent = 1 and is_free=1 and category_type in ('SSC','BANKING','RAILWAY','CTET','DEFENCE/NDA/NAVY')) as a left join course_resource_mapping crm1 on a.assortment_id=crm1.assortment_id and crm1.resource_type like \"assortment\" left join course_resource_mapping crm2 on crm1.course_resource_id=crm2.assortment_id and crm2.resource_type like \"assortment\" left join course_resource_mapping crm3 on crm2.course_resource_id=crm3.assortment_id and crm3.resource_type like \"assortment\" left join course_resource_mapping crm4 on crm3.course_resource_id=crm4.assortment_id and crm4.resource_type like \"resource\" left join course_resources cr on crm4.course_resource_id=cr.id where crm4.live_at is not null and crm4.live_at between ? and ? and crm4.is_replay=0 and cr.resource_type in (1) group by cr.id order by assortment_locale, crm4.live_at desc limit 10";
        return mysql.pool.query(sql, [assortmentId, startTime, endTime]).then((res) => res[0]);
    }

    if (category === "iit_neet") {
        sql = "SELECT cr.*, a.*, crm4.course_resource_id,crm4.name,crm4.resource_type,crm4.live_at, crm3.assortment_id as chapter_assortment from (SELECT DISTINCT assortment_id, display_name,is_free,meta_info as assortment_locale,CONCAT(category,' ',year_exam,' ',meta_info) as course_session FROM course_details WHERE assortment_id = ? and parent = 1 and is_free=1) as a left join course_resource_mapping crm1 on a.assortment_id=crm1.assortment_id and crm1.resource_type like \"assortment\" left join course_resource_mapping crm2 on crm1.course_resource_id=crm2.assortment_id and crm2.resource_type like \"assortment\" left join course_resource_mapping crm3 on crm2.course_resource_id=crm3.assortment_id and crm3.resource_type like \"assortment\" left join course_resource_mapping crm4 on crm3.course_resource_id=crm4.assortment_id and crm4.resource_type like \"resource\" left join course_resources cr on crm4.course_resource_id=cr.id where crm4.live_at is not null and crm4.live_at between ? and ? and crm4.is_replay=0 and cr.resource_type in (1) group by cr.id order by assortment_locale, crm4.live_at desc limit 10";
        return mysql.pool.query(sql, [assortmentId, startTime, endTime]).then((res) => res[0]);
    }
    sql = "SELECT cr.*, a.*, crm4.course_resource_id,crm4.name,crm4.resource_type,crm4.live_at, crm3.assortment_id as chapter_assortment from (SELECT DISTINCT assortment_id, display_name,is_free,meta_info as assortment_locale,CONCAT(year_exam-1,'-',substr(year_exam,3),' ',meta_info) as course_session FROM course_details WHERE assortment_id = ? and parent = 1 and is_free=1 and category_type in ('BOARDS/SCHOOL/TUITION','BANKING','RAILWAY')) as a left join course_resource_mapping crm1 on a.assortment_id=crm1.assortment_id and crm1.resource_type like \"assortment\" left join course_resource_mapping crm2 on crm1.course_resource_id=crm2.assortment_id and crm2.resource_type like \"assortment\" left join course_resource_mapping crm3 on crm2.course_resource_id=crm3.assortment_id and crm3.resource_type like \"assortment\" left join course_resource_mapping crm4 on crm3.course_resource_id=crm4.assortment_id and crm4.resource_type like \"resource\" left join course_resources cr on crm4.course_resource_id=cr.id where crm4.live_at is not null and crm4.live_at between ? and ? and crm4.is_replay=0 and cr.resource_type in (1) group by cr.id order by assortment_locale, crm4.live_at desc limit 10";
    return mysql.pool.query(sql, [assortmentId, startTime, endTime]).then((res) => res[0]);
}

// async function getDemoVideoExperiment(assortmentId) {
//     let sql = "";
//     if (+assortmentId === 15 || +assortmentId === 16) {
//         // 230-270 ms
//         sql = "select a.*,b.live_at,d.*,e.answer_id,e.duration from (select *,case when player_type=\"youtube\" and meta_info is not null then meta_info ELSE resource_reference end as question_id from course_resources cr where resource_type in (1,8)) as a inner join (select * from course_resource_mapping where resource_type=\"resource\") as b on b.course_resource_id=a.id inner join (select assortment_id,is_free,meta_info as course_language from course_details where assortment_type=\"resource_video\" and is_free=1) as d on d.assortment_id=b.assortment_id inner join (select course_resource_id,assortment_id from course_resource_mapping where resource_type=\"assortment\") as c on c.course_resource_id=b.assortment_id and c.assortment_id in (select course_resource_id from course_resource_mapping where assortment_id in (select course_resource_id from course_resource_mapping where assortment_id in (select course_resource_id from course_resource_mapping where assortment_id=?))) left join answers as e on e.question_id=a.question_id group by d.assortment_id";
//         return mysql.pool.query(sql, [assortmentId]).then((res) => res[0]);
//     }
//     // 240-280 ms
//     sql = "select a.*,b.live_at,b.batch_id,d.*,e.answer_id,e.duration, 'demo' as top_title1 from (select *,case when player_type=\"youtube\" and meta_info is not null then meta_info ELSE resource_reference end as question_id from course_resources cr where resource_type in (1,8,9)) as a inner join (select * from course_resource_mapping where resource_type=\"resource\") as b on b.course_resource_id=a.id inner join (select assortment_id,is_free,display_image_square, meta_info as course_language from course_details where assortment_type=\"resource_video\" and is_free=1) as d on d.assortment_id=b.assortment_id inner join (select course_resource_id,assortment_id from course_resource_mapping where resource_type=\"assortment\") as c on c.course_resource_id=b.assortment_id and c.assortment_id in (select course_resource_id from course_resource_mapping where assortment_id in (select course_resource_id from course_resource_mapping where assortment_id=?)) left join answers as e on e.question_id=a.question_id group by d.assortment_id, b.batch_id order by FIELD(subject, 'GUIDANCE','ANNOUNCEMENT') desc";
//     return mysql.pool.query(sql, [assortmentId]).then((res) => res[0]);
// }

async function getBatchesFromAssortment(assortmentId) {
    const sql = "select * from course_assortment_batch_mapping where assortment_id = ? and is_active = 1";
    return mysql.pool.query(sql, [assortmentId]).then((res) => res[0]);
}

async function getSubjectAssortments(assortmentList) {
    const sql = "select * from course_details where assortment_id in (?) and assortment_type = 'subject' and is_active = 1";
    return mysql.pool.query(sql, [assortmentList]).then((res) => res[0]);
}

async function redisHandling(data, type, groupedAssortments, assortmentId) {
    if (_.isEmpty(data)) {
        // may be set herding key ??
        // console.log('empty');
    } else {
    // set in redis with all class combination
        groupedAssortments[assortmentId].map(async () => {
            // console.log(item.class);
            // set data in redis
            await redis.setAsync(`${redisKey[type].key}${assortmentId}`, JSON.stringify(data), "Ex", redisKey[type].expiry);
        });
    }
}

async function getAllAssortmentsRedis(assortmentList) {
    return redis.getAsync(`course_all_assortments_${assortmentList}`);
}

async function setAllAssortmentsRedis(assortmentList, data) {
    return redis.setAsync(`course_all_assortments_${assortmentList}`, JSON.stringify(data), "Ex", 60 * 30);
}

async function getAllAssortments(assortmentList) {
    try {
        let data = [];
        data = await getAllAssortmentsRedis(JSON.stringify(assortmentList));
        if (!_.isNull(data)) {
            return JSON.parse(data);
        }
        data = await helperRecursive.getAllAssortmentsRecursively(assortmentList, []);
        await setAllAssortmentsRedis(JSON.stringify(assortmentList), data);
        return data;
    } catch (e) {
        console.log(e);
        throw new Error(e);
    }
}

async function getLiveSectionFromAssortmentHome(assortmentIds, _studentClass, subject, batchID) {
    let sql = "";
    if (subject && subject !== "ALL") {
        sql = "select *,case when d.faculty_name is null then b.expert_name else d.faculty_name end as mapped_faculty_name,case when d.image_url is NULL then b.expert_image else d.image_url end as image_bg_liveclass, b.stream_status as is_active from (select live_at, assortment_id, course_resource_id from course_resource_mapping where assortment_id in (?) and resource_type='resource' and live_at <=CURRENT_TIMESTAMP and batch_id=?) as a inner join (select assortment_id,is_free from course_details) as e on a.assortment_id=e.assortment_id join (select id, resource_reference, resource_type, subject, topic, expert_name, expert_image, q_order, class, player_type, meta_info, tags, name, display, description, chapter, chapter_order, exam, board, ccm_id, book, faculty_id, stream_start_time, image_url, locale, vendor_id, duration, created_at, created_by, rating, old_resource_id, stream_end_time, stream_push_url, stream_vod_url, stream_status, old_detail_id, lecture_type from course_resources where resource_type in (1,4,8) and subject=?) as b on a.course_resource_id=b.id left join (select name as faculty_name, id,raw_image_url as image_url, degree_obtained from etoos_faculty) as d on b.faculty_id=d.id left join (select duration,question_id,is_vdo_ready from answers) as h on h.question_id=b.resource_reference group by b.id order by a.live_at desc limit 16";
        return mysql.pool.query(sql, [assortmentIds, batchID, subject]).then((res) => res[0]);
    }
    sql = "select *,case when d.faculty_name is null then b.expert_name else d.faculty_name end as mapped_faculty_name,case when d.image_url is NULL then b.expert_image else d.image_url end as image_bg_liveclass, b.stream_status as is_active from (select live_at, assortment_id, course_resource_id from course_resource_mapping where assortment_id in (?) and resource_type='resource' and live_at <=CURRENT_TIMESTAMP and batch_id=?) as a inner join (select assortment_id,is_free from course_details) as e on a.assortment_id=e.assortment_id join (select id, resource_reference, resource_type, subject, topic, expert_name, expert_image, q_order, class, player_type, meta_info, tags, name, display, description, chapter, chapter_order, exam, board, ccm_id, book, faculty_id, stream_start_time, image_url, locale, vendor_id, duration, created_at, created_by, rating, old_resource_id, stream_end_time, stream_push_url, stream_vod_url, stream_status, old_detail_id, lecture_type from course_resources where resource_type in (1,4,8)) as b on a.course_resource_id=b.id left join (select name as faculty_name, id,raw_image_url as image_url, degree_obtained from etoos_faculty) as d on b.faculty_id=d.id left join (select duration,question_id,is_vdo_ready from answers) as h on h.question_id=b.resource_reference group by b.id order by a.live_at desc limit 16";
    return mysql.pool.query(sql, [assortmentIds, batchID]).then((res) => res[0]);
}

async function getLiveClassesByAssortmentID(assortmentID, batchID) {
    const sql = "select * from (select course_resource_id,live_at from course_resource_mapping where live_at > CURRENT_DATE() and live_at < now() and assortment_id in (select course_resource_id from course_resource_mapping where assortment_id in (select course_resource_id from course_resource_mapping where assortment_id in (select course_resource_id from course_resource_mapping where assortment_id=? and resource_type='assortment') and resource_type='assortment') and resource_type='assortment') and resource_type='resource' and batch_id=?) as a inner join (select resource_type,resource_reference,display,stream_status,expert_name,expert_image,id,subject,stream_start_time from course_resources where (resource_type in (1,8) OR (resource_type=4 and stream_status='ACTIVE'))) as b on b.id=a.course_resource_id left join (select question_id, answer_id,duration from answers where question_id<>0) as ans on ans.question_id=b.resource_reference where NOW() < DATE_ADD(a.live_at, INTERVAL ans.duration SECOND) order by a.live_at desc";
    return mysql.pool.query(sql, [assortmentID, batchID]).then((res) => res[0]);
}

async function cacheLiveSectionFromAssortmentHome(assortmentId) {
    const assortmentDetails = {
        assortment_id: assortmentId,
    };
    let allAssortments = await getAllAssortments([assortmentDetails.assortment_id]);
    let subject = null;
    const { assortmentList } = allAssortments;
    allAssortments = allAssortments.totalAssortments;
    if (assortmentList.indexOf(assortmentDetails.assortment_id) < 0) {
        assortmentList.push(parseInt(assortmentDetails.assortment_id));
    }
    if (allAssortments.indexOf(assortmentDetails.assortment_id) < 0) {
        allAssortments.push(parseInt(assortmentDetails.assortment_id));
    }
    const batchIds = await getBatchesFromAssortment(assortmentDetails.assortment_id);
    if (batchIds.length === 0) batchIds.push({ batch_id: 1 });
    for (const batchId of batchIds) {
        console.log(`Caching parent assortment ${assortmentDetails.assortment_id}`);
        let liveSectionData = await getLiveSectionFromAssortmentHome(assortmentList, "", subject, batchId.batch_id);
        liveSectionData = liveSectionData.map((item) => {
            item.students = Math.floor(10000 + Math.random() * 20000);
            item.interested = Math.floor(20000 + Math.random() * 30000);
            return item;
        });
        await redis.setAsync(`${redisKey.course_live_section_home.key}:${assortmentDetails.assortment_id}_${subject}_${batchId.batch_id}`, JSON.stringify(liveSectionData), "Ex", redisKey.course_live_section_home.expiry);
    }
    const subjectAssortments = await getSubjectAssortments(allAssortments);
    for (const subjectDetails of subjectAssortments) {
        let allSubjectAssortments = await getAllAssortments([subjectDetails.assortment_id]);
        const { assortmentList: subjectAssortmentList } = allSubjectAssortments;
        allSubjectAssortments = allSubjectAssortments.totalAssortments;
        if (subjectAssortmentList.indexOf(subjectDetails.assortment_id) < 0) {
            subjectAssortmentList.push(parseInt(subjectDetails.assortment_id));
        }
        subject = subjectDetails.display_name;
        console.log(`Caching subject assortment ${subjectDetails.assortment_id}`);
        const subjectBatchIds = await getBatchesFromAssortment(subjectDetails.assortment_id);
        if (subjectBatchIds.length === 0) subjectBatchIds.push({ batch_id: 1 });
        for (const batchId of subjectBatchIds) {
            let liveSectionData = await getLiveSectionFromAssortmentHome(subjectAssortmentList, "", subject, batchId.batch_id);
            liveSectionData = liveSectionData.map((item) => {
                item.students = Math.floor(10000 + Math.random() * 20000);
                item.interested = Math.floor(20000 + Math.random() * 30000);
                return item;
            });
            await redis.setAsync(`${redisKey.course_live_section_home.key}:${subjectDetails.assortment_id}_${subject}_${batchId.batch_id}`, JSON.stringify(liveSectionData), "Ex", redisKey.course_live_section_home.expiry);
        }
    }
}

async function cacheLiveSectionFromAssortment(db, assortmentId) {
    const batchIds = await getBatchesFromAssortment(assortmentId);
    if (batchIds.length === 0) batchIds.push({ batch_id: 1 });
    for (const batchId of batchIds) {
        const data = await getLiveClassesByAssortmentID(assortmentId, batchId.batch_id);
        await redis.setAsync(`${redisKey.course_live_section.key}:${assortmentId}:${batchId.batch_id}`, JSON.stringify(data), "Ex", redisKey.course_live_section.expiry);
    }
}

async function getRevisionClasses(assortmentId) {
    const sql = "SELECT cr.*, a.*, crm4.course_resource_id,crm4.name,crm4.resource_type,crm4.live_at,crm3.assortment_id as chapter_assortment from (SELECT DISTINCT assortment_id, display_name,is_free,meta_info as assortment_locale,category_type as course_session FROM course_details WHERE assortment_id = ? and parent = 1 and is_free=1) as a left join course_resource_mapping crm1 on a.assortment_id=crm1.assortment_id and crm1.resource_type like \"assortment\" left join course_resource_mapping crm2 on crm1.course_resource_id=crm2.assortment_id and crm2.resource_type like \"assortment\" left join course_resource_mapping crm3 on crm2.course_resource_id=crm3.assortment_id and crm3.resource_type like \"assortment\" left join course_resource_mapping crm4 on crm3.course_resource_id=crm4.assortment_id and crm4.resource_type like \"resource\" left join course_resources cr on crm4.course_resource_id=cr.id left join liveclass_course_details lcd on lcd.id = crm4.is_trial where crm4.live_at is not null and crm4.is_replay=0 and cr.resource_type in (1) and lcd.lecture_type in  ('Revision-One ShotClasses','term_two_classes') group by cr.id order by assortment_locale, crm4.live_at desc";
    return mysql.pool.query(sql, [assortmentId]).then((res) => res[0]);
}

async function start(job) {
    try {
        job.progress(0);
        // get all the combinations of class and assortments
        const assortmentClass = await getCourseAssortmentClassCombination();
        const groupedAssortments = _.groupBy(assortmentClass, "assortment_id");
        const revisionClassAssortmentIds = [159772, 159773, 159774, 159775, 165055, 165056, 165057, 165058];
        // assortment wise data will be same but we will set data in redis based on assortment + class combination
        let totalAssortmentProcessed = 0;
        for (const assortmentId in groupedAssortments) {
            // eslint-disable-next-line no-prototype-builtins
            if (groupedAssortments.hasOwnProperty(assortmentId)) {
                let studentClass = groupedAssortments[assortmentId][0].class;
                for (let i = 0; i < groupedAssortments[assortmentId].length; i++) {
                    if (parseInt(groupedAssortments[assortmentId][i].class) === 14) {
                        studentClass = 14;
                    }
                }
                // check if it is free or not
                // if (groupedAssortments[assortmentId][0].is_free === 0) {
                // paid assortments cache logic will go here
                // } else {
                // free assortments cache logic will go here
                const startTimeBoard = moment().add(5, "hours").add(30, "minutes").subtract(3, "days")
                    .startOf("hour")
                    .format("YYYY-MM-DD HH:mm:ss");
                const endTimeBoard = moment().add(5, "hours").add(30, "minutes").startOf("hour")
                    .format("YYYY-MM-DD HH:mm:ss");
                const startTimeNeet = moment().add(5, "hours").add(30, "minutes").subtract(3, "days")
                    .startOf("hour")
                    .format("YYYY-MM-DD HH:mm:ss");
                const endTimeNeet = moment().add(5, "hours").add(30, "minutes").startOf("hour")
                    .format("YYYY-MM-DD HH:mm:ss");
                const upcomingLectures = await getUpcomingLecturesByAssortmentId(assortmentId);
                const liveNowLectures = await getLiveNowLecturesByAssortmentId(assortmentId);
                const replayLectures = await getReplayLecturesByAssortmentId(assortmentId);
                const boardsLectures = await getRecentLecturesByAssortmentId(assortmentId, startTimeBoard, endTimeBoard, "boards", studentClass);
                const iitNeetLectures = await getRecentLecturesByAssortmentId(assortmentId, startTimeNeet, endTimeNeet, "iit_neet", studentClass);

                if (revisionClassAssortmentIds.includes(parseInt(assortmentId))) {
                    const revisionLectures = await getRevisionClasses(assortmentId);
                    await redisHandling(revisionLectures, "revision_classes", groupedAssortments, assortmentId);
                }
                // const demoVideosData = await getDemoVideoExperiment(mysqlClient, assortmentId);
                await redisHandling(upcomingLectures, "upcoming", groupedAssortments, assortmentId);
                await redisHandling(liveNowLectures, "live_now", groupedAssortments, assortmentId);
                await redisHandling(replayLectures, "replay", groupedAssortments, assortmentId);
                await redisHandling(boardsLectures, "recent_boards", groupedAssortments, assortmentId);
                await redisHandling(iitNeetLectures, "recent_iit_neet", groupedAssortments, assortmentId);
                await cacheLiveSectionFromAssortmentHome(assortmentId);
                await cacheLiveSectionFromAssortment(assortmentId);
                // console.log(assortmentId)
                // console.log(demoVideosData)
                // await redisHandling(demoVideosData, 'course_demo_video', groupedAssortments, assortmentId);
                // }
                totalAssortmentProcessed++;
            }
        }
        console.log("totalAssortmentProcessed");
        console.log(totalAssortmentProcessed);
        console.log("script done");
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
    cron: "*/15 * * * *", // * every 15 min
};
