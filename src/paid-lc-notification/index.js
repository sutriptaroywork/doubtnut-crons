/* eslint-disable guard-for-in */
/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const {
    mysql, config, notification, email, aws,
} = require("../../modules");

async function getUpcomingPaidLC() {
    const sql = `SELECT b.id as resource_id, a.id as detail_id, d.class, a.live_at,a.subject as subject_class, a.liveclass_course_id, a.chapter, b.resource_reference, c.name as faculty_name,
    c.gender as faculty_gender, c.image_url as faculty_image,d.title, d.locale,e.board,e.exam,f.moe_segment_name,concat('q-thumbnail/notif-thumb-', a.id, '-', d.class, '-',
    b.resource_reference,'.png') as thumbnail, d.is_free from course_resources as b 
    left join liveclass_course_details as a on a.id=b.old_detail_id 
    left join dashboard_users as c on a.faculty_id=c.id 
    left JOIN liveclass_course as d on a.liveclass_course_id=d.id 
    left join course_details_liveclass_course_mapping as e on d.id=e.liveclass_course_id 
    left join notification_segment_liveclass as f on d.class=f.class and d.locale=f.locale and e.exam=f.exam and e.board=f.board
    where b.resource_type = 4 and live_at between NOW() - INTERVAL 5 MINUTE and NOW() + INTERVAL 5 MINUTE and a.is_replay = 0 and e.is_free=0 and d.class in (9, 10, 11, 12, 13)`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

/**
 * Get subscribed students
 * @param {number} id Course resource Id
 * @returns {{student_id:number, gcm_reg_id:string}[]}
 */
async function getStudentIdsByCourseResourceId(id) {
    const sql = `SELECT DISTINCT s.student_id, s.gcm_reg_id, b.assortment_id, b.id as package_id from (SELECT * from package where assortment_id in (
        select x.assortment_id from 
        (SELECT T2.*
                        FROM (
                            SELECT
                                @r AS _id,
                                (SELECT @r := assortment_id FROM course_resource_mapping WHERE course_resource_id = _id and 
                                    ((resource_type='resource' and course_resource_id=${id}) 
                                    OR 
                                    (resource_type='assortment' and course_resource_id<>${id})) limit 1) AS assortment_id,
                                @l := @l + 1 AS lvl
                            FROM
                                (SELECT @r := ${id}, @l := 0) vars,
                                course_resource_mapping m
                            WHERE @r <> 0) T1
                        JOIN course_resource_mapping T2 ON T1._id = T2.course_resource_id) x
                        where not (x.course_resource_id=${id} and x.resource_type='assortment') and not (x.course_resource_id<>${id} and x.resource_type='resource')
        )) as b 
        left join student_package_subscription as c on b.id=c.new_package_id
        left join students s on s.student_id=c.student_id
        where c.is_active = 1 order by c.student_id,b.assortment_id, b.id`;
    return mysql.pool.query(sql).then((res) => res[0].map((x) => x.student_id.toString()));
}

async function getUpcomingPaidLCNew() {
    const sql = `
    SELECT e.assortment_id as course_ass_id,d.assortment_id as subject_ass_id, c.assortment_id as chapter_ass_id,a.assortment_id as last_ass_id, b.id as resource_id,
g.class,a.live_at,b.subject as subject_class,b.chapter,b.resource_reference,h.name as faculty_name, 
concat('q-thumbnail/notif-thumb-', lcd.id, '-', g.class, '-',b.resource_reference,'.png') as thumbnail,
h.gender as faculty_gender,g.locale,f.board,f.exam,i.moe_segment_name,b.topic,a.batch_id from 
(SELECT * from course_resource_mapping where date(live_at)=CURRENT_DATE and schedule_type = 'scheduled' and resource_type = 'resource') as a
left join course_resources as b on a.course_resource_id = b.id
left join course_resource_mapping as c on a.assortment_id = c.course_resource_id and c.resource_type = 'assortment'
left join course_resource_mapping as d on c.assortment_id = d.course_resource_id and d.resource_type = 'assortment'
left join course_resource_mapping as e on d.assortment_id = e.course_resource_id and e.resource_type = 'assortment'
left join (SELECT liveclass_course_id,assortment_id,vendor_id,is_free,case when board like 'STATE ENGLISH' then 'CBSE' else board end as board,exam,locale,course_type 
from course_details_liveclass_course_mapping) as f on e.assortment_id=f.assortment_id
left join liveclass_course as g on f.liveclass_course_id=g.id
left join dashboard_users as h on b.faculty_id = h.id
left join liveclass_course_details as lcd on lcd.id=a.is_trial
left join notification_segment_liveclass as i on g.class=i.class and g.locale=i.locale and f.board=i.board and f.exam=i.exam
where b.resource_type in (1,4) and a.live_at between NOW() - INTERVAL 5 MINUTE and NOW() + INTERVAL 5 MINUTE and a.is_replay = 0 and f.vendor_id in (1,2) and f.is_free = 0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

/**
 * Get subscribed students
 * @param {number} assId Assortment Id
 * @returns {{student_id:number, gcm_reg_id:string}[]}
 */
async function getSubscribedStudentIds(assId, batchId) {
    const sql = `select s.student_id, s.gcm_reg_id from student_package_subscription sps 
left join package p on p.id=sps.new_package_id 
left join students s on s.student_id=sps.student_id
where sps.is_active = 1 and sps.end_date>CURRENT_DATE and sps.start_date<=CURRENT_DATE
and p.assortment_id=${assId} and p.batch_id=${batchId}`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

function getMessages(locale) {
    // const sql = "SELECT * from notification_content_liveclass where language=? and is_active=1";
    // return mysql.pool.query(sql, [locale]).then((res) => res[0]);

    if (locale === "HINDI") {
        return [
            {
                title: "क्लास शुरू हो गई है: ##TOPIC##",
                message: "##FACULTY## ##GENDER## ki class start hogayi hai, तुरंत देखो",
            },
            {
                title: "क्लास शुरू हो गई है: ##TOPIC##",
                message: "##FACULTY## ##GENDER## | ##SUBJECT## | तुरंत देखो",
            },
            {
                title: "उपस्थिति का समय: ##TOPIC##",
                message: "##FACULTY## ##GENDER## is LIVE. तुरंत देखो",
            },
            {
                title: "अब देखो: ##TOPIC##",
                message: "Class ##CLASS## | महत्वपूर्ण कक्षा | तुरंत देखो",
            },
            {
                title: "अब देखो: ##FACULTY## ##GENDER##",
                message: "##TOPIC## की क्लास | तुरंत देखो",
            },
        ];
    }
    return [
        {
            title: "Live now : ##TOPIC##",
            message: "##FACULTY## ##GENDER## is live! Join Now",
        },
        {
            title: "Live now : ##TOPIC##",
            message: "##FACULTY## ##GENDER## | ##SUBJECT## | Join Now",
        },
        {
            title: "Watch Live: ##TOPIC##",
            message: "##FACULTY## ##GENDER## is live! Join Now",
        },
        {
            title: "Watch now : ##TOPIC##",
            message: "Class ##CLASS## | IMP Class | Join Now",
        },
        {
            title: "Watch Live: ##FACULTY## ##GENDER##",
            message: "##TOPIC## ki class | Join Now",
        },
    ];
}

function buildMsg(template, data) {
    let gender = "";
    if (data.faculty_gender) {
        if (data.faculty_gender.toLowerCase() === "male") {
            gender = "SIR";
        } else if (data.faculty_gender.toLowerCase() === "female") {
            gender = "MA’AM";
        }
    }
    return template
        .replace("##TOPIC##", data.chapter)
        .replace("##CLASS##", data.class)
        .replace("##FACULTY##", data.faculty_name)
        .replace("##SUBJECT##", data.subject_class)
        .replace("##GENDER##", gender);
}

function prioritize(lcs) {
    const priorities = {
        IIT: ["MATHS", "PHYSICS", "CHEMISTRY", "ENGLISH", "BIOLOGY"],
        NEET: ["BIOLOGY", "PHYSICS", "CHEMISTRY", "ENGLISH", "MATHS"],
    };
    // eslint-disable-next-line guard-for-in
    for (const segmentName in lcs) {
        const lcArr = lcs[segmentName];
        if (lcArr.length === 1) {
            continue;
        }
        const priorityArr = priorities[lcArr[0].exam];
        if (!priorityArr) {
            continue;
        }
        lcArr.sort((a, b) => priorityArr.indexOf(b.subject_class) - priorityArr.indexOf(a.subject_class));
    }
}

async function thumbnailExists(Key) {
    if (!Key) {
        return false;
    }
    try {
        await aws.s3.headObject({
            Bucket: config.staticBucket,
            Key,
        }).promise();
        return true;
    } catch (e) {
        return false;
    }
}

async function handlePaidClasses(job, sentStudents) {
    let paid = 0;
    const paidLcs = await getUpcomingPaidLC();
    const groupedData = _.groupBy(paidLcs, "moe_segment_name");
    prioritize(groupedData);
    const groupedLcs = [];
    for (const segmentName in groupedData) {
        groupedLcs.push({
            moe_segment_name: segmentName,
            class: groupedData[segmentName][0].class,
            data: groupedData[segmentName],
        });
    }
    groupedLcs.sort((a, b) => b.class - a.class);

    for (let i = 0; i < groupedLcs.length; i++) {
        const segmentName = groupedLcs[i].moe_segment_name;
        const rows = groupedLcs[i].data;
        for (let j = 0; j < rows.length; j++) {
            const lc = rows[j];
            const students = await getStudentIdsByCourseResourceId(lc.resource_id);
            const filteredStudents = _.differenceBy(students, sentStudents, "student_id");
            if (!filteredStudents.length) {
                console.log("Skipping", lc.resource_reference, segmentName);
                continue;
            }
            const messages = getMessages(lc.locale);
            const message = _.sample(messages);
            const thumbExist = await thumbnailExists(lc.thumbnail);
            const notificationPayload = {
                event: "video",
                image: (lc.thumbnail && thumbExist) ? `${config.staticCDN}${lc.thumbnail}` : "",
                title: buildMsg(message.title, lc),
                message: buildMsg(message.message, lc),
                firebase_eventtag: `LC_PAID_${lc.resource_reference}_${segmentName}`,
                s_n_id: `LC_PAID_${lc.resource_reference}_${segmentName}`,
                data: JSON.stringify({
                    page: "LIVECLASS_NOTIFICATION",
                    qid: lc.resource_reference,
                }),
            };
            if (parseInt(lc.resource_reference)) {
                await notification.sendNotification(students.map((x) => ({ id: x.student_id, gcmId: x.gcm_reg_id })), notificationPayload);
                email.sendEmail(["paid_user_notif@doubtnut.com"], "LC-PAID", `Notifiication sent for LC_PAID_${lc.resource_reference}_${segmentName}`);
            }
            await job.progress(33 + parseInt((i * 33) / groupedLcs.length));
            paid++;
            // sentStudents.push(...filteredStudents);
        }
    }
    await job.progress(66);
    return paid;
}

function pseudoSort(lcs) {
    const data = {};
    const examPriorities = ["IIT", "NEET", "BOARDS"];
    const subjectPriorities = {
        IIT: ["MATHS", "PHYSICS", "CHEMISTRY", "ENGLISH", "BIOLOGY"],
        NEET: ["BIOLOGY", "PHYSICS", "CHEMISTRY", "ENGLISH", "MATHS"],
    };
    // eslint-disable-next-line guard-for-in
    for (const assId in lcs) {
        const filtered = [];
        const lcArr = lcs[assId];
        if (lcArr.length === 1) {
            data[assId] = lcArr[0];
            continue;
        }
        const groupedData = _.groupBy(lcArr, "exam");
        // eslint-disable-next-line guard-for-in
        for (const exam in groupedData) {
            const examLcArr = groupedData[exam];
            const p = subjectPriorities[exam];
            if (p) {
                examLcArr.sort((a, b) => p.indexOf(b.subject_class) - p.indexOf(a.subject_class));
            }
            filtered.push(examLcArr[0]);
        }
        filtered.sort((a, b) => examPriorities.indexOf(b.exam) - examPriorities.indexOf(a.exam));
        data[assId] = filtered[0];
    }
    return data;
}

async function handlePaidClassesNew(job, sentStudents) {
    let paid = 0;
    const assIdTypes = ["last_ass_id", "chapter_ass_id", "subject_ass_id", "course_ass_id"];
    const paidLcs = await getUpcomingPaidLCNew();

    for (let i = 0; i < assIdTypes.length; i++) {
        const assIdType = assIdTypes[i];
        const groupedData = pseudoSort(_.groupBy(paidLcs, assIdType));
        const c = Object.keys(groupedData);
        for (const assId in groupedData) {
            if (!assId) {
                continue;
            }
            const lc = groupedData[assId];
            const students = await getSubscribedStudentIds(assId, lc.batch_id);
            const filteredStudents = _.differenceBy(students, sentStudents, "student_id");
            if (!filteredStudents.length) {
                console.log("Skipping", lc.resource_reference, assId);
                continue;
            }
            const messages = getMessages(lc.locale);
            const message = _.sample(messages);
            const thumbExist = await thumbnailExists(lc.thumbnail);
            const notificationPayload = {
                event: "video",
                image: (lc.thumbnail && thumbExist) ? `${config.staticCDN}${lc.thumbnail}` : "",
                title: buildMsg(message.title, lc),
                message: buildMsg(message.message, lc),
                firebase_eventtag: `LC_PAID_${lc.resource_reference}_${assId}`,
                s_n_id: `LC_PAID_${lc.resource_reference}_${assId}`,
                data: JSON.stringify({
                    page: "LIVECLASS_NOTIFICATION",
                    qid: parseInt(lc.resource_reference),
                }),
            };
            if (parseInt(lc.resource_reference)) {
                await notification.sendNotification(students.map((x) => ({ id: x.student_id, gcmId: x.gcm_reg_id })), notificationPayload);
                email.sendEmail(["paid_user_notif@doubtnut.com"], "LC-PAID", `Notifiication sent for LC_PAID_${lc.resource_reference}_${assId}`);
            }
            await job.progress(66 + parseInt((c.indexOf(assId) * 33 * (i + 1)) / (c.length * 4)));
            paid++;
            // sentStudents.push(...filteredStudents);
        }
    }

    await job.progress(99);
    return paid;
}

async function start(job) {
    let paid = 0;
    const sentStudents = [];
    try {
        paid = await handlePaidClassesNew(job, sentStudents);
        paid = await handlePaidClasses(job, sentStudents);

        await job.progress(100);
        return {
            data: {
                paid,
            },
        };
    } catch (err) {
        console.error(err);
        return { err };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "14,29,44,59 5-22 * * *",
};
