/* eslint-disable no-useless-concat */
/* eslint-disable no-unused-vars */
/* eslint-disable no-await-in-loop */
const moment = require("moment");

function getClassDurationCount(database, assortment_id) {
    // let sql = `SELECT count(*) as count, sum(t2.duration)/60 as duration from (SELECT distinct a.assortment_id, a.display_name,c.resource_reference from (Select * from course_details where assortment_id in (SELECT course_resource_id FROM course_resource_mapping WHERE assortment_id = ${assortment_id})) as a left join course_resource_mapping as b on a.assortment_id = b.assortment_id  left join course_resource_mapping as d on b.course_resource_id = d.assortment_id left join (SELECT id,resource_type, resource_reference from course_resources where resource_type in (1,4,8)) as c on d.course_resource_id = c.id where d.resource_type='resource' and c.id is not null ORDER BY b.resource_type  DESC) as t1 left join answers as t2 on t1.resource_reference = t2.question_id`;
    const sql = `SELECT count(t1.resource_reference) as count, sum(t2.duration)/60 as duration  from (SELECT * from course_resources where id in (SELECT course_resource_id from course_resource_mapping where assortment_id in (SELECT course_resource_id from course_resource_mapping WHERE assortment_id in (SELECT course_resource_id  FROM course_resource_mapping WHERE assortment_id in (SELECT course_resource_id  FROM course_resource_mapping WHERE assortment_id = ${assortment_id} ORDER BY live_at  ASC)))) and resource_type in (1,4,8)) as t1 left join answers as t2 on t1.resource_reference = t2.question_id`;
    return database.query(sql);
}

function getClassPdf(database, assortment_id) {
    // let sql =`SELECT a.assortment_id, a.display_name,d.course_resource_id,d.resource_type from (Select * from course_details where assortment_id in (SELECT course_resource_id FROM course_resource_mapping WHERE assortment_id = ${assortment_id})) as a left join course_resource_mapping as b on a.assortment_id = b.assortment_id  left join course_resource_mapping as d on b.course_resource_id = d.assortment_id left join (SELECT id,resource_type, resource_reference from course_resources where resource_type in (2)) as c on d.course_resource_id = c.id where d.resource_type='resource' and c.id is not null ORDER BY b.resource_type  DESC`;
    const sql = `SELECT t1.resource_reference from (SELECT * from course_resources where id in (SELECT course_resource_id from course_resource_mapping where assortment_id in (SELECT course_resource_id from course_resource_mapping WHERE assortment_id in (SELECT course_resource_id  FROM course_resource_mapping WHERE assortment_id in (SELECT course_resource_id  FROM course_resource_mapping WHERE assortment_id = ${assortment_id} ORDER BY live_at  ASC)))) and resource_type in (2)) as t1`;
    return database.query(sql);
}

function getChapterDurationCount(database, assortment_id) {
    // let sql = `Select count(*) as count, sum(t2.duration)/60 as duration from (SELECT distinct a.assortment_id, c.resource_reference from (SELECT *  FROM course_details WHERE assortment_id = ${assortment_id}) as a left join course_resource_mapping as b on a.assortment_id = b.assortment_id left join (SELECT id, case when player_type = 'youtube' then meta_info else resource_reference end as resource_reference from course_resources where resource_type in (1,4,8)) as c on b.course_resource_id = c.id) as t1 left join answers as t2 on t1.resource_reference = t2.question_id`;
    const sql = `SELECT count(t1.resource_reference) as count,sum(t2.duration)/60 as duration from (SELECT case when player_type = 'youtube' then meta_info else resource_reference end as resource_reference from course_resources where id in (SELECT course_resource_id from course_resource_mapping where assortment_id in (SELECT course_resource_id  FROM course_resource_mapping WHERE assortment_id = ${assortment_id} ORDER BY live_at  ASC)) and resource_type in (1,4,8) ORDER BY id  ASC) as t1 left JOIN answers as t2 on t1.resource_reference = t2.question_id`;
    return database.query(sql);
}

function getSubjectDurationCount(database, assortment_id) {
    // let sql = `SELECT count(*) as count, sum(t2.duration)/60 as duration from (SELECT distinct a.assortment_id, a.display_name,c.resource_reference from (Select * from course_details where assortment_id in (SELECT course_resource_id FROM course_resource_mapping WHERE assortment_id = ${assortment_id})) as a left join course_resource_mapping as b on a.assortment_id = b.assortment_id left join (SELECT id, case when player_type = 'youtube' then meta_info else resource_reference end as resource_reference from course_resources where resource_type in (1,4,8)) as c on b.course_resource_id = c.id where b.resource_type='resource' and c.id is not null ORDER BY b.resource_type  DESC) as t1 left join answers as t2 on t1.resource_reference=t2.question_id`;
    const sql = `SELECT count(t1.resource_reference) as count, sum(t2.duration)/60 as duration from (SELECT case when player_type = 'youtube' then meta_info else resource_reference end as resource_reference from course_resources where id in (SELECT course_resource_id from course_resource_mapping where assortment_id in (SELECT course_resource_id from course_resource_mapping where assortment_id in (SELECT course_resource_id  FROM course_resource_mapping WHERE assortment_id = ${assortment_id} ORDER BY live_at  ASC))) and resource_type in (1,4,8) ORDER BY id  ASC) as t1 left JOIN answers as t2 on t1.resource_reference = t2.question_id`;
    return database.query(sql);
}

function updateAssortment(database, assortment_id) {
    const sql = `update course_details set is_free=0 where assortment_id=${assortment_id}`;
    return database.query(sql);
}

function getChapterPdf(database, assortment_id) {
    // let sql =`SELECT distinct a.assortment_id,c.id,c.resource_type, c.resource_reference from (SELECT *  FROM course_details WHERE assortment_id = ${assortment_id} ) as a left join course_resource_mapping as b on a.assortment_id = b.assortment_id left join (SELECT id,resource_type, resource_reference from course_resources where resource_type in (2)) as c on b.course_resource_id = c.id where b.resource_type='resource' and c.id is not null`;
    const sql = `SELECT id from course_resources where id in (SELECT course_resource_id from course_resource_mapping where assortment_id in (SELECT course_resource_id  FROM course_resource_mapping WHERE assortment_id = ${assortment_id} ORDER BY live_at  ASC)) and resource_type in (2)`;
    return database.query(sql);
}

function getSubjectPdf(database, assortment_id) {
    // let sql =` SELECT a.assortment_id, a.display_name,b.course_resource_id,b.resource_type from (Select * from course_details where assortment_id in (SELECT course_resource_id FROM course_resource_mapping WHERE assortment_id = ${assortment_id})) as a left join course_resource_mapping as b on a.assortment_id = b.assortment_id  left join (SELECT id,resource_type, resource_reference from course_resources where resource_type in (2)) as c on b.course_resource_id = c.id where b.resource_type='resource' and c.id is not null ORDER BY b.resource_type  DESC`;
    const sql = `SELECT id from course_resources where id in (SELECT course_resource_id from course_resource_mapping where assortment_id in (SELECT course_resource_id from course_resource_mapping where assortment_id in (SELECT course_resource_id  FROM course_resource_mapping WHERE assortment_id = ${assortment_id} ORDER BY live_at  ASC))) and resource_type in (2) ORDER BY id  ASC`;
    return database.query(sql);
}

function getSubjectTests(database, assortment_id) {
    // let sql =`SELECT a.assortment_id, a.display_name,b.course_resource_id,b.resource_type from (Select * from course_details where assortment_id in (SELECT course_resource_id FROM course_resource_mapping WHERE assortment_id = ${assortment_id})) as a left join course_resource_mapping as b on a.assortment_id = b.assortment_id  left join (SELECT id,resource_type, resource_reference from course_resources where resource_type in (9)) as c on b.course_resource_id = c.id where b.resource_type='resource' and c.id is not null ORDER BY b.resource_type  DESC`;
    const sql = `SELECT count(id) from course_resources where id in (SELECT course_resource_id from course_resource_mapping where assortment_id in (SELECT course_resource_id from course_resource_mapping where assortment_id in (SELECT course_resource_id  FROM course_resource_mapping WHERE assortment_id = ${assortment_id} ORDER BY live_at  ASC))) and resource_type in (9) ORDER BY id  ASC`;
    return database.query(sql);
}

function getType(database, res_id) {
    const sql = `select distinct assortment_id, assortment_type,category, display_name from course_details where assortment_id = ${res_id}`;
    return database.query(sql);
}

function getCourseMapping(database, assortment_id) {
    const sql = `select * from course_resource_mapping where assortment_id=${assortment_id}`;
    return database.query(sql);
}

function getAllFreeCourse(database) {
    // let sql =`select distinct assortment_id from course_details where is_free=0 and assortment_type='course' and assortment_id =58144`;
    // const sql = 'select distinct assortment_id from course_details where course_details.assortment_id > 113696 and  is_free=0 and assortment_type  not in (\'chapter\', \'resource_video\', \'resource_pdf\', \'resource_test\',\'resource_quiz\')';
    const sql = "SELECT distinct a.assortment_id from (SELECT *  FROM course_details WHERE assortment_id > 113696 and assortment_type not in ('chapter', 'resource_video', 'resource_pdf', 'resource_test','resource_quiz') and is_free = 0) as a left join package as b on a.assortment_id = b.assortment_id where b.id is null order by a.assortment_id asc";

    return database.query(sql);
}

function getAllFreeCourseChapter(database) {
    // let sql =`select distinct assortment_id from course_details where is_free=0 and assortment_type='subject' and assortment_id not in (SELECT assortment_id from package group by assortment_id)`;
    const sql = "select distinct assortment_id from course_details where is_free=0 and assortment_type='subject' and assortment_id not in (SELECT assortment_id from package group by assortment_id)";
    return database.query(sql);
}

function getAllFreeCourseSubjectFromClass(database) {
    const sql = "select distinct assortment_id from course_details where is_free=0 and assortment_type='class' and assortment_id not in (SELECT assortment_id from package group by assortment_id)";
    return database.query(sql);
}

function getChapterNotes(database, assortment_id) {
    const sql = `SELECT distinct a.assortment_id,c.id,c.resource_type, c.resource_reference from (SELECT *  FROM course_details WHERE assortment_id = ${assortment_id} ) as a left join course_resource_mapping as b on a.assortment_id = b.assortment_id left join (SELECT id,resource_type, resource_reference from course_resources where resource_type in (2)) as c on b.course_resource_id = c.id where b.resource_type='resource' and c.id is not null`;
    return database.query(sql);
}

function getChapterTests(database, assortment_id) {
    const sql = `SELECT distinct a.assortment_id,c.id,c.resource_type, c.resource_reference from (SELECT *  FROM course_details WHERE assortment_id = ${assortment_id} ) as a left join course_resource_mapping as b on a.assortment_id = b.assortment_id left join (SELECT id,resource_type, resource_reference from course_resources where resource_type in (9)) as c on b.course_resource_id = c.id where b.resource_type='resource' and c.id is not null`;
    return database.query(sql);
}

function getChapterDuration(database, assortment_id) {
    const sql = `Select sum(t2.duration) as secs from (SELECT distinct a.assortment_id, c.resource_reference from (SELECT *  FROM course_details WHERE assortment_id = ${assortment_id}) as a left join course_resource_mapping as b on a.assortment_id = b.assortment_id left join (SELECT id, case when player_type = 'youtube' then meta_info else resource_reference end as resource_reference from course_resources where resource_type in (1,4,8)) as c on b.course_resource_id = c.id) as t1 left join answers as t2 on t1.resource_reference = t2.question_id`;
    return database.query(sql);
}

function getAllCourseResource(database, course_resource_id) {
    const sql = `select * from course_resources where id=${course_resource_id}`;
    return database.query(sql);
}

function getAllAssortmentType(database, assortment_id) {
    const sql = `select * from (select * from course_resource_mapping where assortment_id=${assortment_id}) as a left join course_resources on a.course_resource_id=b.id where b.id is not null`;
    return database.query(sql);
}

function getCourseResource(database, course_resource_id) {
    const sql = `select case when player_type = 'youtube' then meta_info else resource_reference end as resource_reference, description, expert_name from course_resources where id=${course_resource_id}  limit 1`;
    return database.query(sql);
}

function toTitleCase(str) {
    return str.replace(
        /\w\S*/g,
        (txt) => txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase(),
    );
}

function getAssortmentType(database, assortment_id) {
    const sql = `select * from course_resource_mapping where assortment_id=${assortment_id} limit 1`;
    console.log(sql);
    return database.query(sql);
}

function getDuration(database, question_id) {
    const sql = `select * from answers where question_id=${question_id} order by answer_id DESC`;
    return database.query(sql);
}

function getChapterResources(database) {
    // let sql = `SELECT * FROM (select * from course_details where assortment_id >=1541 and assortment_type = 'resource_video') as a left join (select assortment_id, schedule_type,course_resource_id from course_resource_mapping) as b on a.assortment_id=b.assortment_id left join (select * from course_resources where resource_type=4) as c on b.course_resource_id=c.id where b.assortment_id is not null and c.id is not null`;
    // let sql = `SELECT * FROM (select * from course_details where assortment_id >=1541 and assortment_type = 'resource_video') as a left join (select assortment_id, schedule_type,course_resource_id from course_resource_mapping) as b on a.assortment_id=b.assortment_id where b.assortment_id is not null`;
    const sql = "select * from course_details where assortment_type = 'chapter' and is_free=0 order by assortment_id ASC";
    console.log(sql);
    return database.query(sql);
}

function getVideoResources(database) {
    // let sql = `SELECT * FROM (select * from course_details where assortment_id >=1541 and assortment_type = 'resource_video') as a left join (select assortment_id, schedule_type,course_resource_id from course_resource_mapping) as b on a.assortment_id=b.assortment_id left join (select * from course_resources where resource_type=4) as c on b.course_resource_id=c.id where b.assortment_id is not null and c.id is not null`;
    // let sql = `SELECT * FROM (select * from course_details where assortment_id >=1541 and assortment_type = 'resource_video') as a left join (select assortment_id, schedule_type,course_resource_id from course_resource_mapping) as b on a.assortment_id=b.assortment_id where b.assortment_id is not null`;
    const sql = "SELECT distinct a.assortment_id, a.category, a.display_name from (SELECT *  FROM course_details WHERE assortment_id > 113696 and assortment_type = 'resource_video' and is_free = 0) as a left join package as b on a.assortment_id = b.assortment_id where b.id is null order by a.assortment_id asc";
    console.log(sql);
    return database.query(sql);
}
function getPDFResources(database) {
    // let sql = `SELECT * FROM (select * from course_details where assortment_id >=1541 and assortment_type = 'resource_video') as a left join (select assortment_id, schedule_type,course_resource_id from course_resource_mapping) as b on a.assortment_id=b.assortment_id left join (select * from course_resources where resource_type=4) as c on b.course_resource_id=c.id where b.assortment_id is not null and c.id is not null`;
    // let sql = `SELECT * FROM (select * from course_details where assortment_id >=1541 and assortment_type = 'resource_video') as a left join (select assortment_id, schedule_type,course_resource_id from course_resource_mapping) as b on a.assortment_id=b.assortment_id where b.assortment_id is not null`;
    const sql = "SELECT distinct a.assortment_id, a.category, a.display_name from (SELECT *  FROM course_details WHERE assortment_type = 'resource_pdf' and is_free = 0) as a left join package as b on a.assortment_id = b.assortment_id where b.id is null order by a.assortment_id asc";
    console.log(sql);
    return database.query(sql);
}

function getTestResources(database) {
    const sql = "SELECT distinct a.assortment_id, a.category, a.display_name from (SELECT *  FROM course_details WHERE assortment_type = 'resource_test' and is_free = 0) as a left join package as b on a.assortment_id = b.assortment_id where b.id is null order by a.assortment_id asc";
    console.log(sql);
    return database.query(sql);
}

function insertPackage(database, obj) {
    const sql = "insert into package set ?";
    return database.query(sql, [obj]);
}

function insertVariants(database, obj) {
    const sql = "insert into variants set ?";
    return database.query(sql, [obj]);
}

function checkAssortmentInPackages(database, aId) {
    const sql = `select * from package where assortment_id=${aId}`;
    return database.query(sql);
}

async function main(db) {
    try {
        const chapterResources = await getVideoResources(db);
        console.log(chapterResources);
        for (let i = 0; i < chapterResources.length; i++) {
            const assortmentType = await getAssortmentType(db, chapterResources[i].assortment_id);
            const check = await checkAssortmentInPackages(db, chapterResources[i].assortment_id);
            if (assortmentType && assortmentType.length > 0 && !check.length) {
                if (assortmentType[0].schedule_type == "recorded") {
                    const courseResource = await getCourseResource(db, assortmentType[0].course_resource_id);
                    const categoryArray = chapterResources[i].category.split("|");
                    const nameArray = chapterResources[i].display_name.split("|");
                    const duration = await getDuration(db, courseResource[0].resource_reference);
                    let minutes = 10;
                    if (duration && duration.length > 0 && duration[0].duration != null && duration[0].duration > 0) {
                        minutes = Math.floor(duration[0].duration / 60);
                    }
                    const name = `Single Video | ${toTitleCase(nameArray[1])} | ${toTitleCase(nameArray[2])} for ${categoryArray[0].toUpperCase()}`;
                    const obj = {
                        assortment_id: chapterResources[i].assortment_id,
                        name,
                        description: `${toTitleCase(courseResource[0].description.replace(/\|/g, " | "))} | ${minutes} mins+`,
                        is_active: 1,
                        type: "subscription",
                        min_limit: 19,
                        duration_in_days: 365,
                    };
                    const insert = await insertPackage(db, obj);
                    const varObj = {
                        package_id: insert.insertId,
                        base_price: 29,
                        display_price: 29,
                        is_default: 1,
                        is_show: 1,
                        is_active: 1,
                    };
                    console.log(obj);
                    await insertVariants(db, varObj);
                } else if (assortmentType[0].schedule_type == "scheduled") {
                    const courseResource = await getCourseResource(db, assortmentType[0].course_resource_id);
                    console.log(assortmentType);
                    const description = toTitleCase(courseResource[0].description.replace(/\|/g, " | "));
                    const categoryArray = chapterResources[i].category.split("|");
                    const nameArray = chapterResources[i].display_name.split("|");
                    let name = `Single Video | ${toTitleCase(nameArray[1])} | ${toTitleCase(nameArray[2])} for ${categoryArray[0].toUpperCase()}`;
                    if (courseResource && courseResource[0] && courseResource[0].expert_name != null && courseResource[0].expert_name != "") {
                        name = `${name} by ${toTitleCase(courseResource[0].expert_name)}`;
                    }
                    const obj = {
                        assortment_id: chapterResources[i].assortment_id,
                        name,
                        description: `${toTitleCase(courseResource[0].description.replace(/\|/g, " | "))} | ` + ` Class live on ${moment(assortmentType[0].live_at).format("dddd, MMMM Do YYYY, h:mm:ss a")}`,
                        is_active: 1,
                        type: "subscription",
                        min_limit: 19,
                        duration_in_days: 365,
                    };
                    console.log(obj);
                    const insert = await insertPackage(db, obj);
                    const varObj = {
                        package_id: insert.insertId,
                        base_price: 29,
                        display_price: 29,
                        is_default: 1,
                        is_show: 1,
                        is_active: 1,
                    };
                    await insertVariants(db, varObj);
                }
            }
        }
        // const pdfResources = await getPDFResources(db);
        // console.log(pdfResources);
        //
        // for (let i = 0; i < pdfResources.length; i++) {
        //     const assortmentType = await getAssortmentType(db, pdfResources[i].assortment_id);
        //     const check = await checkAssortmentInPackages(db, pdfResources[i].assortment_id);
        //     if (assortmentType && assortmentType.length > 0 && !check.length) {
        //         if (assortmentType[0].schedule_type == 'recorded') {
        //             const courseResource = await getCourseResource(db, assortmentType[0].course_resource_id);
        //             const categoryArray = pdfResources[i].category.split('|');
        //             const nameArray = pdfResources[i].display_name.split('|');
        //             const name = `${nameArray[0]} | ${toTitleCase(nameArray[1])} | ${toTitleCase(nameArray[2])} for ${categoryArray[0].toUpperCase()}`;
        //             const obj = {
        //                 assortment_id: pdfResources[i].assortment_id,
        //                 name,
        //                 description: toTitleCase(courseResource[0].description.replace(/\|/g, ' | ')),
        //                 is_active: 1,
        //                 type: 'subscription',
        //                 min_limit: 5,
        //                 duration_in_days: 365,
        //             };
        //             const insert = await insertPackage(db, obj);
        //             const varObj = {
        //                 package_id: insert.insertId,
        //                 base_price: 15,
        //                 display_price: 5,
        //                 is_default: 1,
        //                 is_show: 1,
        //                 is_active: 1,
        //             };
        //             console.log(obj);
        //             await insertVariants(db, varObj);
        //         } else if (assortmentType[0].schedule_type == 'scheduled') {
        //             const courseResource = await getCourseResource(db, assortmentType[0].course_resource_id);
        //             const description = toTitleCase(courseResource[0].description.replace(/\|/g, ' | '));
        //             const categoryArray = pdfResources[i].category.split('|');
        //             const nameArray = pdfResources[i].display_name.split('|');
        //             const name = `${nameArray[0]} | ${toTitleCase(nameArray[1])} | ${toTitleCase(nameArray[2])} for ${categoryArray[0].toUpperCase()}`;
        //             const obj = {
        //                 assortment_id: pdfResources[i].assortment_id,
        //                 name,
        //                 description: toTitleCase(courseResource[0].description.replace(/\|/g, ' | ')),
        //                 is_active: 1,
        //                 type: 'subscription',
        //                 min_limit: 5,
        //                 duration_in_days: 365,
        //             };
        //             console.log(obj);
        //             const insert = await insertPackage(db, obj);
        //             const varObj = {
        //                 package_id: insert.insertId,
        //                 base_price: 15,
        //                 display_price: 5,
        //                 is_default: 1,
        //                 is_show: 1,
        //                 is_active: 1,
        //             };
        //             await insertVariants(db, varObj);
        //         }
        //     }
        // }
        //
        // const testResources = await getTestResources(db);
        // for (let i = 0; i < testResources.length; i++) {
        //     const assortmentType = await getAssortmentType(db, testResources[i].assortment_id);
        //     const check = await checkAssortmentInPackages(db, testResources[i].assortment_id);
        //     if (assortmentType && assortmentType.length > 0 && !check.length) {
        //         if (assortmentType[0].schedule_type == 'recorded') {
        //             const courseResource = await getCourseResource(db, assortmentType[0].course_resource_id);
        //             const categoryArray = testResources[i].category.split('|');
        //             const nameArray = testResources[i].display_name.split('|');
        //             const name = `${nameArray[0]} | ${toTitleCase(nameArray[1])} | ${toTitleCase(nameArray[2])} for ${categoryArray[0].toUpperCase()}`;
        //             const obj = {
        //                 assortment_id: testResources[i].assortment_id,
        //                 name,
        //                 description: toTitleCase(courseResource[0].description.replace(/\|/g, ' | ')),
        //                 is_active: 1,
        //                 type: 'subscription',
        //                 min_limit: 5,
        //                 duration_in_days: 365,
        //             };
        //             const insert = await insertPackage(db, obj);
        //             const varObj = {
        //                 package_id: insert.insertId,
        //                 base_price: 15,
        //                 display_price: 5,
        //                 is_default: 1,
        //                 is_show: 1,
        //                 is_active: 1,
        //             };
        //             console.log(obj);
        //             await insertVariants(db, varObj);
        //         } else if (assortmentType[0].schedule_type == 'scheduled') {
        //             const courseResource = await getCourseResource(db, assortmentType[0].course_resource_id);
        //             const description = toTitleCase(courseResource[0].description.replace(/\|/g, ' | '));
        //             const categoryArray = testResources[i].category.split('|');
        //             const nameArray = testResources[i].display_name.split('|');
        //             const name = `${nameArray[0]} | ${toTitleCase(nameArray[1])} | ${toTitleCase(nameArray[2])} for ${categoryArray[0].toUpperCase()}`;
        //             const obj = {
        //                 assortment_id: testResources[i].assortment_id,
        //                 name,
        //                 description: toTitleCase(courseResource[0].description.replace(/\|/g, ' | ')),
        //                 is_active: 1,
        //                 type: 'subscription',
        //                 min_limit: 5,
        //                 duration_in_days: 365,
        //             };
        //             console.log(obj);
        //             const insert = await insertPackage(db, obj);
        //             const varObj = {
        //                 package_id: insert.insertId,
        //                 base_price: 15,
        //                 display_price: 5,
        //                 is_default: 1,
        //                 is_show: 1,
        //                 is_active: 1,
        //             };
        //             await insertVariants(db, varObj);
        //         }
        //     }
        // }

        const allFreeCourse = await getAllFreeCourse(db);
        for (let i = 0; i < allFreeCourse.length; i++) {
            console.log(i);
            const courseMapping = await getCourseMapping(db, allFreeCourse[i].assortment_id);
            if (courseMapping && courseMapping.length > 0) {
                for (let j = 0; j < courseMapping.length; j++) {
                    console.log(j);
                    const type = await getType(db, courseMapping[j].course_resource_id);
                    const check = await checkAssortmentInPackages(db, courseMapping[j].course_resource_id);
                    if (type && type.length && !check.length) {
                        if (type[0].assortment_type == "class") {
                            const categoryArray = type[0].category.split("|");
                            const name = `${toTitleCase(type[0].display_name)} for ${categoryArray[0].toUpperCase()}`;
                            const classPdf = await getClassPdf(db, type[0].assortment_id);
                            const obj = {
                                assortment_id: type[0].assortment_id,
                                name,
                                description: `Master complete class for ${categoryArray[0].toUpperCase()}`,
                                is_active: 1,
                                type: "subscription",
                                min_limit: 59,
                                duration_in_days: 365,
                            };
                            if (courseMapping[j].schedule_type == "recorded") {
                                const duration = await getClassDurationCount(db, type[0].assortment_id);
                                if (duration && duration.length > 0 && duration[0].duration != 0 && duration[0].count != 0) {
                                    obj.description = `${obj.description} | ${duration[0].count} classes - RECORDED - ${Math.floor(duration[0].duration)}+ minutes`;
                                }
                            } else {
                                obj.description = `${obj.description} | ` + "Regular Live Classes Every Week | Recording Available";
                            }
                            if (classPdf && classPdf.length > 0) {
                                obj.description = `${obj.description} - ${classPdf.length} PDF`;
                            }

                            console.log(obj);
                            const insert = await insertPackage(db, obj);
                            const varObj = {
                                package_id: insert.insertId,
                                base_price: 6000,
                                display_price: 5000,
                                is_default: 1,
                                is_show: 1,
                                is_active: 1,
                            };
                            await insertVariants(db, varObj);
                            // await updateAssortment(type[0].assortment_id);
                        } else if (type[0].assortment_type == "subject") {
                            const categoryArray = type[0].category.split("|");
                            const name = `${toTitleCase(type[0].display_name)} for ${categoryArray[0].toUpperCase()}`;
                            const subjectTests = await getSubjectTests(db, type[0].assortment_id);
                            const subjectPdf = await getSubjectPdf(db, type[0].assortment_id);
                            const obj = {
                                assortment_id: type[0].assortment_id,
                                name,
                                description: `Master complete subject for ${categoryArray[0].toUpperCase()}`,
                                is_active: 1,
                                type: "subscription",
                                min_limit: 59,
                                duration_in_days: 365,
                            };
                            if (courseMapping[j].schedule_type == "recorded") {
                                const duration = await getSubjectDurationCount(db, type[0].assortment_id);
                                if (duration && duration.length > 0 && duration[0].duration != 0 && duration[0].count != 0) {
                                    // obj.description = obj.description + ' | ' + Math.floor(duration[0].duration) + '+ minutes of video on demand | '+duration[0].count+ ' videos on demand'
                                    obj.description = `${obj.description} | ${duration[0].count} classes - RECORDED - ${Math.floor(duration[0].duration)}+ minutes`;
                                }
                            } else {
                                obj.description = `${obj.description} | ` + "Regular Live Classes Every Week | Recording Available";
                            }
                            if (subjectTests && subjectTests.length > 0) {
                                obj.description = `${obj.description} - ${subjectTests.length} Tests`;
                            }
                            if (subjectPdf && subjectPdf.length > 0) {
                                obj.description = `${obj.description} - ${subjectPdf.length} PDF`;
                            }

                            console.log(obj);
                            const insert = await insertPackage(db, obj);
                            const varObj = {
                                package_id: insert.insertId,
                                base_price: 1000,
                                display_price: 699,
                                is_default: 1,
                                is_show: 1,
                                is_active: 1,
                            };
                            await insertVariants(db, varObj);
                            // await updateAssortment(type[0].assortment_id);
                        } else if (type[0].assortment_type == "chapter") {
                            const categoryArray = type[0].category.split("|");
                            const name = `${toTitleCase(type[0].display_name)} for ${categoryArray[0].toUpperCase()}`;
                            const chapterTests = await getChapterTests(db, type[0].assortment_id);
                            const chapterPdf = await getChapterPdf(db, type[0].assortment_id);
                            const obj = {
                                assortment_id: type[0].assortment_id,
                                name,
                                description: `All lectures in this series related to this chapter for ${categoryArray[0].toUpperCase()}`,
                                is_active: 1,
                                type: "subscription",
                                min_limit: 59,
                                duration_in_days: 365,
                            };
                            if (courseMapping[j].schedule_type == "recorded") {
                                const duration = await getChapterDurationCount(db, type[0].assortment_id);
                                if (duration && duration.length > 0 && duration[0].duration != 0 && duration[0].count != 0) {
                                    // obj.description = obj.description + ' | ' + Math.floor(duration[0].duration) + '+ minutes of video on demand | '+duration[0].count+ ' videos on demand'
                                    obj.description = `${obj.description} | ${duration[0].count} classes - RECORDED - ${Math.floor(duration[0].duration)}+ minutes`;
                                }
                            } else {
                                obj.description = `${obj.description} | ` + "Regular Live Classes Every Week | Recording Available";
                            }
                            if (chapterTests && chapterTests.length > 0) {
                                obj.description = `${obj.description} - ${chapterTests.length} Tests`;
                            }
                            if (chapterPdf && chapterPdf.length > 0) {
                                obj.description = `${obj.description} - ${chapterPdf.length} PDF`;
                            }
                            console.log(obj);
                            const insert = await insertPackage(db, obj);
                            const varObj = {
                                package_id: insert.insertId,
                                base_price: 229,
                                display_price: 159,
                                is_default: 1,
                                is_show: 1,
                                is_active: 1,
                            };
                            await insertVariants(db, varObj);
                            // await updateAssortment(type[0].assortment_id);
                        }
                    }
                }
            }
        }
        return 1;
    } catch (e) {
        console.log(e);
        return e;
    } finally {
        console.log(`the script successfully ran at ${new Date()}`);
    }
}
module.exports = {
    main,
};

// is_free=0 and type='course'
// assortment_id : course_mapping  iterate : course_details : type
