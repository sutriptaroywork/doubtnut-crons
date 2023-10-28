/* eslint-disable guard-for-in */
const _ = require("lodash");
const { redis, redshift, mysql } = require("../../modules");


async function setSubjectsList(studentClass, metaInfo, subject) {
    return redis.set(`FCLP:SUBJECTS:${studentClass}:${metaInfo}`, JSON.stringify(subject), "Ex", 60 * 60 * 24);// 1 day
}

async function setTeachersList(studentClass, metaInfo, subject, chapters, teachers) {
    return redis.set(`FCLP:TEACHERS:${studentClass}:${metaInfo}:${subject}:${chapters}`, JSON.stringify(teachers), "Ex", 60 * 60 * 24);// 1 day
}

async function setChaptersList(studentClass, metaInfo, subject, chapters) {
    return redis.set(`FCLP:CHAPTERS:${studentClass}:${metaInfo}:${subject}`, JSON.stringify(chapters), "Ex", 60 * 60 * 24);// 1 day
}

async function setSubjectAssortmentIds(studentClass, metaInfo, subject, subjectAssortments) {
    return redis.set(`FCLP:SUBJECT_ASSORTMENTS:${studentClass}:${metaInfo}:${subject}`, JSON.stringify(subjectAssortments), "Ex", 60 * 60 * 24);// 1 day
}

async function getChapterData(studentClass, metaInfo) {
    if (+studentClass === 14) {
        const sql = `select
                distinct
            cd_course.assortment_id as course_assortment_id,
            cd_course.display_name as course_name,
            cd_subject.assortment_id as subject_assortment_id,
            cd_subject.display_name as subject_name,
            cd_chapter.assortment_id  as chapter_assortment_id,
            cd_chapter.display_name as chapter_name,
            cr.expert_name,
            cr.faculty_id,
            cr.resource_type  
        from
            classzoo1.course_details as cd_course
        inner join classzoo1.course_resource_mapping crm_subject on
            crm_subject.assortment_id = cd_course.assortment_id
            and cd_course.class = ${+studentClass}
            and cd_course.is_active = 1
            and cd_course.assortment_type = 'course'
            and cd_course.is_free = 1
            and category_type in ('BOARDS/SCHOOL/TUITION' ,'SSC','DEFENCE/NDA/NAVY','RAILWAY','BANKING','CTET')            
            and cd_course.assortment_id in (159772, 159773, 159774, 159775, 165049, 165050, 165051, 165052, 165053, 165054, 165055, 165056, 165057, 165058, 330514, 330515, 330516, 330517, 330518, 330519, 330520, 330521, 23, 31, 324960, 324961, 344177)
        inner join classzoo1.course_details cd_subject on
            cd_subject.assortment_id = crm_subject.course_resource_id
            and cd_subject.display_name not in ('ALL', 'WEEKLY TEST', 'GUIDANCE', 'QUIZ', 'ANNOUNCEMENT')
            and cd_subject.is_active >0
        INNER join classzoo1.course_resource_mapping crm_chapter on crm_chapter.assortment_id =cd_subject.assortment_id  
        INNER join classzoo1.course_details cd_chapter on crm_chapter.course_resource_id = cd_chapter.assortment_id
        INNER join classzoo1.course_resource_mapping crm_resource_assortment on crm_chapter.course_resource_id  =crm_resource_assortment.assortment_id
        INNER join classzoo1.course_resource_mapping crm_resource on crm_resource_assortment.course_resource_id  =crm_resource.assortment_id  
        INNER join classzoo1.course_resources cr on cr.id = crm_resource.course_resource_id and cr.resource_type in (1,4,8)`;// 300 ms
        return mysql.pool.query(sql).then((x) => x[0]);
    }
    if (+studentClass >= 6 && +studentClass <= 13) {
        const sql = `select distinct
        cd_course.assortment_id as course_assortment_id,
        cd_course.display_name as course_name,
        cd_subject.assortment_id as subject_assortment_id,
        cd_subject.display_name as subject_name,
        cd_chapter.assortment_id  as chapter_assortment_id,
        cd_chapter.display_name as chapter_name,
        cr.expert_name,cr.faculty_id,
        cr.resource_type  
    from
    classzoo1.course_details as cd_course
    inner join classzoo1.course_resource_mapping crm_subject on
        crm_subject.assortment_id = cd_course.assortment_id
        and cd_course.class = ${+studentClass}
        and cd_course.is_active = 1
        and cd_course.assortment_type = 'course'
        and cd_course.is_free = 1
        and cd_course.meta_info in ('${metaInfo}','HINGLISH')
        and cd_course.assortment_id in (159772, 159773, 159774, 159775, 165049, 165050, 165051, 165052, 165053, 165054, 165055, 165056, 165057, 165058, 330514, 330515, 330516, 330517, 330518, 330519, 330520, 330521, 23, 31, 324960, 324961, 344177)
    inner join classzoo1.course_details cd_subject on
        cd_subject.assortment_id = crm_subject.course_resource_id
        and cd_subject.display_name not in ('ALL', 'WEEKLY TEST', 'GUIDANCE', 'QUIZ', 'ANNOUNCEMENT')
        and cd_subject.is_active >0
    INNER join classzoo1.course_resource_mapping crm_chapter on crm_chapter.assortment_id =cd_subject.assortment_id  
    INNER join classzoo1.course_details cd_chapter on crm_chapter.course_resource_id = cd_chapter.assortment_id
    INNER join classzoo1.course_resource_mapping crm_resource_assortment on crm_chapter.course_resource_id  =crm_resource_assortment.assortment_id
    INNER join classzoo1.course_resource_mapping crm_resource on crm_resource_assortment.course_resource_id  =crm_resource.assortment_id  
    INNER join classzoo1.course_resources cr on cr.id = crm_resource.course_resource_id and cr.resource_type in (1,4,8) order by cd_chapter.assortment_id desc`;// 100 ms
        return mysql.pool.query(sql).then((x) => x[0]);
    }
}

async function start(job) {
    try {
        job.progress(0);
        const promises = [];
        const classList = [6, 7, 8, 9, 10, 11, 12, 13, 14];
        const metaInfo = ["HINDI", "ENGLISH"];
        for (let i = 0; i < classList.length; i++) {
            const studentClass = classList[i];
            for (let j = 0; j < metaInfo.length; j++) {
                const meta = metaInfo[j];
                // eslint-disable-next-line no-await-in-loop
                let result = await getChapterData(studentClass, meta);
                result = result.filter((item) => item.course_assortment_id && item.course_name && item.subject_assortment_id && item.subject_name && item.chapter_assortment_id && item.expert_name);
                const dataGroupedBySubject = _.groupBy(result, "subject_name");
                const subjects = Object.keys(dataGroupedBySubject);
                promises.push(setSubjectsList(studentClass, meta, subjects));
                for (const subject in dataGroupedBySubject) {
                    let subjectAssortments = [];
                    dataGroupedBySubject[subject].forEach((item) => subjectAssortments.push(item.subject_assortment_id));
                    let allChapterAssortments = [];

                    dataGroupedBySubject[subject].forEach((item) => allChapterAssortments.push(item.chapter_assortment_id));
                    allChapterAssortments = _.uniq(allChapterAssortments);
                    subjectAssortments = _.uniq(subjectAssortments);
                    promises.push(setSubjectAssortmentIds(studentClass, meta, subject, { subject_assortments: subjectAssortments, chapter_assortments: allChapterAssortments }));

                    const chapterData = _.groupBy(dataGroupedBySubject[subject], "chapter_assortment_id");
                    const chapters = Object.keys(chapterData);
                    const chapterDataToBeStored = [];
                    chapters.forEach((chapter) => {
                        chapterDataToBeStored.push({ chapter_assortment_id: chapter, chapter_name: chapterData[chapter][0].chapter_name });
                    });
                    promises.push(setChaptersList(studentClass, meta, subject, chapterDataToBeStored));

                    for (const chapter in chapterData) {
                        const teachersData = _.groupBy(chapterData[chapter], "faculty_id");
                        const teachers = Object.keys(teachersData);
                        const teachersDataToBeStored = [];
                        teachers.forEach((teacher) => {
                            teachersDataToBeStored.push({ faculty_id: teacher, faculty_name: teachersData[teacher][0].expert_name });
                        });
                        promises.push(setTeachersList(studentClass, meta, subject, chapter, teachersDataToBeStored));
                    }
                }
            }
        }
        await Promise.all(promises);
        job.progress(100);
    } catch (err) {
        console.log(err);
        return { err };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 4 */3 * *", // * Every 3 days at 4 AM
};
