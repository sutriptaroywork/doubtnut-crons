/* eslint-disable guard-for-in */
/* eslint-disable no-await-in-loop */
const moment = require("moment");
const _ = require("lodash");
const { mysql, aws, config } = require("../../modules");
const { createCSVSegment, scheduleNotification } = require("./moe-helper");
const { staticBucket, staticCDN } = require("../../modules/config");

async function thumbExists(Key) {
    return aws.s3.headObject({
        Bucket: staticBucket,
        Key,
    }).promise().then(() => `${staticCDN}${Key}`).catch(() => false);
}

async function getUpcomingPaidLC() {
    const sql = `SELECT b.id as resource_id, a.id as detail_id, d.class, a.live_at,a.subject as subject_class, a.liveclass_course_id, a.chapter, b.resource_reference, c.name as faculty_name,
    c.gender as faculty_gender, c.image_url as faculty_image,d.title, d.locale,e.board,e.exam,f.moe_segment_name,concat('${config.staticCDN}', 'q-thumbnail/notif-thumb-', a.id, '-', d.class, '-',
    b.resource_reference,'.png') as thumbnail, d.is_free from course_resources as b 
    left join liveclass_course_details as a on a.id=b.old_detail_id 
    left join dashboard_users as c on a.faculty_id=c.id 
    left JOIN liveclass_course as d on a.liveclass_course_id=d.id 
    left join course_details_liveclass_course_mapping as e on d.id=e.liveclass_course_id 
    left join notification_segment_liveclass as f on d.class=f.class and d.locale=f.locale and e.exam=f.exam and e.board=f.board
    where b.resource_type = 4 and live_at between NOW() + INTERVAL 45 MINUTE and NOW() + INTERVAL 60 MINUTE and a.is_replay = 0 and e.is_free=0 and d.class in (9, 10, 11, 12, 13)`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getUpcomingFreeLC() {
    const sql = `SELECT b.id as resource_id, a.id as detail_id, d.class, a.live_at,a.subject as subject_class, a.liveclass_course_id, a.chapter, b.resource_reference, c.name as faculty_name,
    c.gender as faculty_gender, c.image_url as faculty_image,d.title, d.locale,e.board,e.exam,concat('${config.staticCDN}', 'q-thumbnail/notif-thumb-', a.id, '-', d.class, '-',
    b.resource_reference,'.png') as thumbnail, d.is_free from course_resources as b 
    left join liveclass_course_details as a on a.id=b.old_detail_id 
    left join dashboard_users as c on a.faculty_id=c.id 
    left JOIN liveclass_course as d on a.liveclass_course_id=d.id 
    left join course_details_liveclass_course_mapping as e on d.id=e.liveclass_course_id 
    where ((e.vendor_id = 1 and b.resource_type in (1,4,8)) or (e.vendor_id = 2 and b.resource_type = 1)) 
    and live_at between NOW() + INTERVAL 45 MINUTE and NOW() + INTERVAL 60 MINUTE and a.is_replay = 0 and e.is_free=1`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

// Getting previous day classes and class of same day conducted atleast 5 hours before and is not replay
async function getCurrentPlusPreviousLC() {
    const sql = `SELECT b.id as resource_id, a.id as detail_id, d.class, a.live_at,a.subject as subject_class, a.liveclass_course_id, a.chapter, b.resource_reference, c.name as faculty_name,
    c.gender as faculty_gender, c.image_url as faculty_image,d.title, d.locale,e.board,e.exam, d.is_free from course_resources as b 
    left join liveclass_course_details as a on a.id=b.old_detail_id 
    left join dashboard_users as c on a.faculty_id=c.id 
    left JOIN liveclass_course as d on a.liveclass_course_id=d.id 
    left join course_details_liveclass_course_mapping as e on d.id=e.liveclass_course_id 
    where b.resource_type in (1,4) and live_at between NOW() - INTERVAL 1080 MINUTE and NOW() - INTERVAL 300 MINUTE and a.is_replay = 0 and e.is_free=1`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getStudentIdsByCourseResourceId(id) {
    const sql = `SELECT DISTINCT c.student_id, b.assortment_id, b.id as package_id from (SELECT * from package where assortment_id in (
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
        left join student_package_subscription as c on b.id=c.new_package_id where c.is_active = 1 order by c.student_id,b.assortment_id, b.id`;
    return mysql.pool.query(sql).then((res) => res[0].map((x) => x.student_id.toString()));
}

async function getUpcomingPaidLCNew() {
    const sql = `
    SELECT e.assortment_id as course_ass_id,d.assortment_id as subject_ass_id, c.assortment_id as chapter_ass_id,a.assortment_id as last_ass_id, b.id as resource_id,
g.class,a.live_at,b.subject as subject_class,b.chapter,b.resource_reference,h.name as faculty_name, 
h.gender as faculty_gender,g.locale,f.board,f.exam,i.moe_segment_name,b.topic from 
(SELECT * from course_resource_mapping where date(live_at)=CURRENT_DATE and schedule_type = 'scheduled' and resource_type = 'resource') as a
left join course_resources as b on a.course_resource_id = b.id
left join course_resource_mapping as c on a.assortment_id = c.course_resource_id and c.resource_type = 'assortment'
left join course_resource_mapping as d on c.assortment_id = d.course_resource_id and d.resource_type = 'assortment'
left join course_resource_mapping as e on d.assortment_id = e.course_resource_id and e.resource_type = 'assortment'
left join (SELECT liveclass_course_id,assortment_id,vendor_id,is_free,case when board like 'STATE ENGLISH' then 'CBSE' else board end as board,exam,locale,course_type 
from course_details_liveclass_course_mapping) as f on e.assortment_id=f.assortment_id
left join liveclass_course as g on f.liveclass_course_id=g.id
left join dashboard_users as h on b.faculty_id = h.id
left join notification_segment_liveclass as i on g.class=i.class and g.locale=i.locale and f.board=i.board and f.exam=i.exam
where b.resource_type in (1,4) and a.live_at between NOW() + INTERVAL 45 MINUTE and NOW() + INTERVAL 60 MINUTE and a.is_replay = 0 and f.vendor_id in (1,2) and f.is_free = 0
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getSubscribedStudentIds(assId) {
    const sql = `select student_id from student_package_subscription sps 
left join package p on p.id=sps.new_package_id 
where sps.is_active = 1 and sps.end_date>CURRENT_DATE and sps.start_date<=CURRENT_DATE
and p.assortment_id=${assId}`;
    return mysql.pool.query(sql).then((res) => res[0].map((x) => x.student_id.toString()));
}

function getSegments() {
    const sql = "SELECT * from notification_segment_liveclass";
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

async function createAndUploadCSV(liveAt, questionId, segmentName, ids) {
    let csvData = "";
    ids.forEach((x) => {
        csvData = `${csvData}${x}\n`;
    });
    csvData = csvData.substr(0, csvData.length - 1);

    const Key = `MOE-CSV/VIP/${moment(liveAt).format("YYYY/MM/DD/HH/mm")}/${questionId}-${segmentName}.csv`;
    try {
        await aws.s3.putObject({
            Bucket: "doubtnut-static",
            Body: csvData,
            Key,
            ContentType: "text/plain",
        }).promise();
    } catch (e) {
        console.error(e);
    }
    return `${config.staticCloudfrontCDN}${Key}`;
}

async function handleFreeClasses(job) {
    let free = 0;
    let liveAt = moment().add("5:30").startOf("h").add(60, "m")
        .toDate();
    // Free classes
    const baseSegment = await getSegments();
    const lcs = await getUpcomingFreeLC();
    liveAt = (lcs.length ? lcs[0].live_at : liveAt) || liveAt;
    const lcsCurrentPlusPreviousday = await getCurrentPlusPreviousLC();

    // checking is there are any live classes being conducted at this hour if yes the this flow is activated
    for (const i in baseSegment) {
        free++;
        const segment = baseSegment[i];
        let scheduled = false;
        // Checking is there is a live class for the segment
        for (const lc of lcs) {
            const thumbnails = [
                `q-thumbnail/notif-thumb-custom-${lc.detail_id}-${lc.class}-${lc.resource_reference}.png`,
                `q-thumbnail/notif-thumb-${lc.detail_id}-${lc.class}-${lc.resource_reference}.png`,
            ];
            const thumbs = await Promise.all(thumbnails.map((x) => thumbExists(x)));
            if (lc.class === segment.class && lc.locale === segment.locale && lc.board === segment.board && lc.exam === segment.exam) {
                const messages = getMessages(lc.locale);
                const message = _.sample(messages);
                await scheduleNotification(segment.moe_segment_name, lc.live_at, lc.resource_reference, thumbs[0] || thumbs[1], {
                    msg: buildMsg(message.message, lc),
                    title: buildMsg(message.title, lc),
                }, "live");
                scheduled = true;
                break;
            }
        }
        // incase no live class scheduling a previous day class for the segment
        if (!scheduled) {
            for (const lc of lcsCurrentPlusPreviousday) {
                if (lc.class === segment.class && lc.locale === segment.locale && lc.board === segment.board && lc.exam === segment.exam) {
                    const messages = getMessages(lc.locale);
                    const message = _.sample(messages);
                    await scheduleNotification(segment.moe_segment_name, liveAt, lc.resource_reference, lc.thumbnail, {
                        msg: buildMsg(message.message, lc),
                        title: buildMsg(message.title, lc),
                    }, "replay");
                    scheduled = true;
                    break;
                }
            }
        }
        await job.progress(parseInt((i * 50) / baseSegment.length));
    }
    await job.progress(50);
    return free;
}

async function handlePaidClasses(job, sentStudentIds) {
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
            const studentIds = await getStudentIdsByCourseResourceId(lc.resource_id);
            const filteredIds = _.difference(studentIds, sentStudentIds);
            if (!filteredIds.length) {
                console.log("Skipping", lc.resource_reference, segmentName);
                continue;
            }

            const csvPath = await createAndUploadCSV(lc.live_at, lc.resource_reference, segmentName, filteredIds);
            if (!csvPath) {
                continue;
            }
            const segName = await createCSVSegment(lc.resource_reference, segmentName, csvPath);
            if (!segName) {
                continue;
            }
            const messages = getMessages(lc.locale);
            const message = _.sample(messages);
            scheduleNotification(segName, lc.live_at, lc.resource_reference, lc.thumbnail, {
                msg: buildMsg(message.message, lc),
                title: buildMsg(message.title, lc),
            }, csvPath);
            await job.progress(33 + parseInt((i * 33) / groupedLcs.length));
            paid++;
            sentStudentIds.push(...filteredIds);
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

async function handlePaidClassesNew(job, sentStudentIds) {
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
            const studentIds = await getSubscribedStudentIds(assId);
            const filteredIds = _.difference(studentIds, sentStudentIds);
            if (!filteredIds.length) {
                console.log("Skipping", lc.resource_reference, assId);
                continue;
            }
            const csvPath = await createAndUploadCSV(lc.live_at, lc.resource_reference, assId, filteredIds);
            if (!csvPath) {
                continue;
            }
            const segName = await createCSVSegment(lc.resource_reference, assId, csvPath);
            if (!segName) {
                continue;
            }
            const messages = getMessages(lc.locale);
            const message = _.sample(messages);
            scheduleNotification(segName, lc.live_at, lc.resource_reference, lc.thumbnail, {
                msg: buildMsg(message.message, lc),
                title: buildMsg(message.title, lc),
            }, csvPath);
            await job.progress(66 + parseInt((c.indexOf(assId) * 33 * (i + 1)) / (c.length * 4)));
            paid++;
            sentStudentIds.push(...filteredIds);
        }
    }

    await job.progress(99);
    return paid;
}

async function start(job) {
    let free = 0;
    // let paid = 0;
    // const sentStudentIds = [];
    try {
        free = await handleFreeClasses(job);
        // paid = await handlePaidClassesNew(job, sentStudentIds);
        // paid = await handlePaidClasses(job, sentStudentIds);

        await job.progress(100);
        return {
            data: {
                free,
                // paid,
            },
        };
    } catch (err) {
        console.error(err);
        return { err };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "10,25,40,55 5-22 * * *",
};
