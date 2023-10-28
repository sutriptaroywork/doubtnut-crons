/* eslint-disable no-await-in-loop */
const moment = require("moment");
const _ = require("lodash");
const {
    mysql, kafka, config,
} = require("../../modules");

async function fetchStudents() {
    const sql = "select a.*, b.assortment_id, d.gcm_reg_id, c.meta_info, d.mobile, c.demo_video_thumbnail, e.display_price, c.parent, d.app_version, d.is_online from (select * from student_package_subscription where start_date < CURRENT_DATE and end_date >= CURRENT_DATE and amount <> -1 and is_active=1 and new_package_id is not null and variant_id is not null and updated_by NOT LIKE 'AS DNK OLD PACKAGE MIGRATION' ) as a left join package as b on a.new_package_id=b.id left join  (select * from course_details where assortment_type in ('class', 'course')) as c on b.assortment_id=c.assortment_id left join students as d on a.student_id=d.student_id left join variants as e on a.variant_id=e.id where c.assortment_id is not null group by a.created_at";
    console.log(sql);
    return mysql.pool.query(sql).then(([res]) => res);
}

async function fetchMigratedStudents() {
    const sql = "select a.*, b.assortment_id, d.gcm_reg_id, c.meta_info, d.mobile, c.demo_video_thumbnail, e.display_price, c.parent, d.app_version, d.is_online from (select * from student_package_subscription where start_date < CURRENT_DATE and end_date >= CURRENT_DATE and amount <> -1 and is_active=1 and new_package_id is not null and variant_id is not null and updated_by LIKE 'AS DNK OLD PACKAGE MIGRATION') as a left join package as b on a.new_package_id=b.id left join  (select * from course_details where assortment_type in ('class', 'course')) as c on b.assortment_id=c.assortment_id left join students as d on a.student_id=d.student_id left join variants as e on a.variant_id=e.id where c.assortment_id is not null group by a.student_id, c.class, c.meta_info";
    console.log(sql);
    return mysql.pool.query(sql).then(([res]) => res);
}

async function getQuestionIdsFromAssortmentId(assortment_id) {
    const sql = `select b.resource_reference as question_id from (select course_resource_id,assortment_id,live_at from course_resource_mapping where assortment_id in (select course_resource_id from course_resource_mapping where assortment_id in (select course_resource_id from course_resource_mapping where assortment_id in (select course_resource_id from course_resource_mapping where assortment_id in (${assortment_id}) and resource_type='assortment') and resource_type='assortment') and resource_type='assortment') and resource_type='resource') as a inner join (select id,meta_info,resource_reference from course_resources where resource_type in (1,4,8)) as b on b.id=a.course_resource_id`;
    return mysql.pool.query(sql).then(([res]) => res);
}

async function getVideoViews(data) {
    const sql = `select student_id, created_at, CAST(question_id AS CHAR(50)) as question_id from video_view_stats where student_id=${data.student_id} and created_at >= '${moment(data.start_date).format("YYYY-MM-DD HH:mm:ss")}'`;
    return mysql.pool.query(sql).then(([res]) => res);
}

function getPayload(student) {
    const stu = {};
    stu.id = student.student_id;
    stu.gcmId = student.gcm_reg_id;
    const appVersion = student.is_online;
    let notificationPayload;
    if (appVersion >= 878) {
        let tag;
        if (appVersion >= 906) {
            tag = student.meta_info === "HINDI" ? "ABSENT_1D_HI_S" : "ABSENT_1D_EN_S";
        } else {
            tag = student.meta_info === "HINDI" ? "1D_ABSENT_HI_S" : "1D_ABSENT_EN_S";
        }
        notificationPayload = {
            event: "course_notification",
            firebase_eventtag: tag,
            sn_type: "text",
            title: student.meta_info === "HINDI" ? "आपके टीचर से एक संदेश आया है 🎙️" : "Aapke teacher ka message aaya hai 🎙️",
            message: student.meta_info === "HINDI" ? "सुनने के लिए क्लिक करें ▶️" : "Click to play ▶️",
            data: {
                id: student.meta_info === "HINDI" ? 100 : 101,
                image_url: `${config.staticCDN}engagement_framework/170A60A2-62D4-FB41-459B-9A64D2F2DE15.webp`,
                is_vanish: true,
                deeplink_banner: student.meta_info === "HINDI" ? "doubtnutapp://video?qid=645233725" : "doubtnutapp://video?qid=645233724",
                offset: 21600 * 1000,
            },
        };
    } else {
        notificationPayload = {
            event: "video",
            firebase_eventtag: student.meta_info === "HINDI" ? "1D_ABSENT_HI" : "1D_ABSENT_EN",
            title: student.meta_info === "HINDI" ? "आपके टीचर से एक संदेश आया है 🎙️" : "Aapke teacher ka message aaya hai 🎙️",
            message: student.meta_info === "HINDI" ? "सुनने के लिए क्लिक करें ▶️" : "Click to play ▶️",
            data: {
                qid: student.meta_info === "HINDI" ? "645233725" : "645233724",
                page: "INAPP",
            },
        };
    }
    return [stu, notificationPayload];
}

async function processStudent(assortmentQuestions, student) {
    try {
        let hasAttended = false;
        let hasClasses = false;
        for (let i = 0; i < student.length; i++) {
            if (!assortmentQuestions[student[i].assortment_id] || !assortmentQuestions[student[i].assortment_id].length) {
                continue;
            }
            hasClasses = true;
            const videoViews = await getVideoViews(student[i]);
            const intersection = _.intersectionBy(videoViews, assortmentQuestions[student[i].assortment_id], "question_id");
            if (intersection.length) {
                const diffDays = moment().diff(moment(intersection[intersection.length - 1].created_at), "days");
                if (!(diffDays != 0 && diffDays > 1)) {
                    hasAttended = true;
                }
            }
        }
        if ((hasClasses && !hasAttended) && student[0].gcm_reg_id) {
            const data = getPayload(student[0]);
            const stu = data[0];
            const notificationPayload = data[1];
            await kafka.sendNotification([stu.student_id], [stu.gcmId], notificationPayload);
        }
    } catch (e) {
        console.error(e);
    }
}

async function start(job) {
    try {
        const assortmentQuestions = {};
        let students = await fetchStudents();
        const migratedStudents = await fetchMigratedStudents();
        students = _.concat(students, migratedStudents);
        const flags = {};
        const studentUnique = students.filter((item) => {
            if (flags[item.student_id]) {
                return false;
            }
            flags[item.student_id] = true;
            return true;
        });
        const result = studentUnique.map((a) => a.student_id);
        console.log(result);
        for (let i = 0; i < students.length; i++) {
            if (!assortmentQuestions[students[i].assortment_id]) {
                const assQuestions = await getQuestionIdsFromAssortmentId(students[i].assortment_id);
                assortmentQuestions[students[i].assortment_id] = assQuestions;
            }
        }
        const arr = [];
        for (let i = 0; i < result.length; i++) {
            const arr2 = students.filter((item) => item.student_id == result[i]);
            arr.push(arr2);
        }
        let c = 0;
        while (arr.length) {
            const s = arr.splice(0, 100);
            await Promise.all(s.map((x) => processStudent(assortmentQuestions, x)));
            c += 100;
            await job.progress(parseInt(((c + 1) * 100) / arr.length));
        }
        return { data: null };
    } catch (e) {
        console.log(e);
        return { err: e };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 7 * * 0,2-6",
    removeOnComplete: 10,
    removeOnFail: 20,
};
