const axios = require('axios');
const http = require('http');
const https = require('https');
const AWS = require('aws-sdk');
const moment = require("moment");
const redisClient = require("../../modules/redis");
const { mysql } = require("../../modules");
const { assert } = require('console');

AWS.config.update({
    httpOptions: {
        agent: new https.Agent({ keepAlive: true, maxSockets: 200, timeout: 180 }),
    },
});

const pznInst = axios.create({
    httpAgent: new http.Agent({ keepAlive: true, maxSockets: 50 }),
    httpsAgent: new https.Agent({ keepAlive: true, maxSockets: 50 }),
});

async function getQuestionByMaxEngageTime(queryData) {
    const data = await pznInst({
        method: "get",
        headers: { "Content-Type": "application/json" },
        url: "https://pzn.internal.doubtnut.com/api/v1/get-top-videos/question-id-by-sum-engage-time",
        timeout: 2500,
        data: queryData,
    });
    return data && data.data ? data.data : null;
}

async function getSubjectListByTotalEt(studentId, startDate, endDate) {
    const data = await pznInst({
        method: "get",
        headers: { "Content-Type": "application/json" },
        url: "https://pzn.internal.doubtnut.com/api/v1/get-top-videos/subject-by-sum-engage-time",
        timeout: 2500,
        data: {
            student_id: `${studentId}`,
            start_date: `${startDate}`,
            end_date: `${endDate}`,
        },
    });
    return data && data.data ? data.data : null;
}

async function getAssortmentsByResourceReference(database, resourceReference, studentClass) {
    let sql;
    if (studentClass) {
        sql = "select distinct(b.assortment_id), d.assortment_id as chapter_assortment, crm3.assortment_id as subject_assortment, c.is_free, c.parent, a.id from (select id from course_resources where resource_reference=? and resource_type in (1,4,8)) as a inner join (select assortment_id, course_resource_id, resource_type  from course_resource_mapping where resource_type='resource') as b on a.id=b.course_resource_id left join (select assortment_id, course_resource_id, resource_type  from course_resource_mapping where resource_type='assortment') as d on b.assortment_id=d.course_resource_id left join (select assortment_id, course_resource_id from course_resource_mapping where resource_type='assortment') as crm3 on crm3.course_resource_id = d.assortment_id left join (select * from course_details where class = ?) as c on c.assortment_id=b.assortment_id";
        // console.log(sql);
        return database.query(sql, [resourceReference.toString(), studentClass]);
    }
    sql = "select distinct(b.assortment_id), d.assortment_id as chapter_assortment, crm3.assortment_id as subject_assortment,c.is_free,c.parent, cd.is_free as is_chapter_free, a.*,cd.class from (select id, subject, expert_image, expert_name, display,chapter, faculty_id from course_resources where resource_reference=? and resource_type in (1,4,8)) as a inner join (select assortment_id, course_resource_id, resource_type  from course_resource_mapping where resource_type='resource') as b on a.id=b.course_resource_id left join (select assortment_id, course_resource_id, resource_type  from course_resource_mapping where resource_type='assortment') as d on b.assortment_id=d.course_resource_id left join (select assortment_id, course_resource_id from course_resource_mapping where resource_type='assortment') as crm3 on crm3.course_resource_id = d.assortment_id left join (select * from course_details) as c on c.assortment_id=b.assortment_id left join (select assortment_id,is_free,class from course_details) as cd on cd.assortment_id=d.assortment_id";
    return database.query(sql, [resourceReference.toString()]);
}

async function getByKey(key, client) {
    return client.getAsync(key);
}

async function setByKey(key, client, data) {
    return client.setAsync(key, JSON.stringify(data), "Ex", 60 * 60);
}

async function getVideoData(studentClass, subjectList, lang, exam) {
    let data = await getByKey(`SCHOLARSHIP_FREE_CLASS_${subjectList[0]}_${studentClass}_${exam}_${lang}`, redisClient);
    data = JSON.parse(data);
    if (!data) {
        const qids = await getQuestionByMaxEngageTime({
            class: +studentClass, subjects: subjectList, language: lang, start_date: moment().subtract(7, 'days').format('YYYY-MM-DD HH:mm:ss'), end_date: moment().format('YYYY-MM-DD HH:mm:ss'), target_group: exam, resource_types: ['1', '4', '8'],
        });
        data = [];
        if (qids.length > 0) {
            data = await getAssortmentsByResourceReference(mysql.pool, qids[0]);
            data[0][0].question_id = qids[0];
        }
        await setByKey(`SCHOLARSHIP_FREE_CLASS_${subjectList[0]}_${studentClass}_${exam}_${lang}`, redisClient, data);
    }
    return data;
}

module.exports = {
    getQuestionByMaxEngageTime,
    getSubjectListByTotalEt,
    getAssortmentsByResourceReference,
    getVideoData,
};
