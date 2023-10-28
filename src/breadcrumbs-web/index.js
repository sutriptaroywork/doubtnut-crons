const _ = require("lodash");
const redisClient = require("../../modules/redis");
const { redshift } = require("../../modules");

// async function getWebQNAVideoDetails() {
//     const sql = "SELECT p.package_language ,q.class,q.subject,q.chapter,q.question_id,q.curtimestamp,a.chapter_trans FROM classzoo1.questions q join classzoo1.studentid_package_mapping_new p on q.student_id = p.student_id and q.student_id < 100 and p.to_index = 1 AND p.content_format = 'QNA VIDEOS' left join classzoo1.chapter_alias_all_lang a on q.chapter = a.chapter and q.class = a.class and q.subject = a.subject where q.student_id < 100 and (is_answered = 1 or is_text_answered = 1) and is_skipped = 0 and matched_question is null and p.to_index = 1 and q.class in (6,7,8,9,10,11,12) AND p.content_format = 'QNA VIDEOS' and p.package_language in ('en','hi','bn','ta','te','gu','pu') and q.subject not in ('NCERT_NEET_VIDEOS','SCIENCE','GENERAL KNOWLEDGE AND APTITUDE','INFORMATION TECHNOLOGY','MOTIVATIONAL VIDEOS') group by 1,2,3,4,5,6,7 order by 6 DESC";
//     return redshift.query(sql).then((res) => res);
// }

async function getLangClassSubjectDetails() {
    const sql = "SELECT p.package_language, q.class, q.subject FROM classzoo1.questions q join classzoo1.studentid_package_mapping_new p on q.student_id = p.student_id and q.student_id  < 100 and p.to_index = 1 where q.student_id < 100 and (is_answered = 1 or is_text_answered = 1) and is_skipped = 0 and matched_question is null and p.to_index = 1 and q.class in (6,7,8,9,10,11,12) and p.package_language in ('en','hi','bn','ta','te','gu','pu') and p.content_format  = 'QNA VIDEOS' and q.subject not in ('NCERT_NEET_VIDEOS','SCIENCE','GENERAL KNOWLEDGE AND APTITUDE','INFORMATION TECHNOLOGY','MOTIVATIONAL VIDEOS','object(Zend_Db_Adapter_Exception)#141') group by 1,2,3 order by 2";
    return redshift.query(sql).then((res) => res);
}

async function getWebQNAVideoDetails(language, qClass, subject) {
    const sql = `SELECT 
    p.package_language,
    q.class,
    q.subject,
    q.chapter,
    q.question_id,
    q.curtimestamp,
    a.chapter_trans 
    FROM classzoo1.questions q join classzoo1.studentid_package_mapping_new p
        on q.student_id = p.student_id and q.student_id < 100 and p.to_index = 1 AND p.content_format = 'QNA VIDEOS'
            left join classzoo1.chapter_alias_all_lang a 
            on q.chapter = a.chapter and q.class = a.class and q.subject = a.subject 
                where q.student_id < 100 and (is_answered = 1 or is_text_answered = 1) and is_skipped = 0 and matched_question is null and p.to_index = 1 
                    and q.class in (${qClass}) AND p.content_format = 'QNA VIDEOS' and p.package_language in ('${language}') 
                    and q.subject in ('${subject}') and q.subject not in ('NCERT_NEET_VIDEOS','SCIENCE','GENERAL KNOWLEDGE AND APTITUDE','INFORMATION TECHNOLOGY','MOTIVATIONAL VIDEOS') 
                    group by 1,2,3,4,5,6,7 order by 6 DESC`;
    return redshift.query(sql).then((res) => res);
}

async function start(job) {
    const allowedSubjects = ["MATHS", "CHEMISTRY", "PHYSICS", "BIOLOGY", "ENGLISH", "ECONOMICS", "ACCOUNTS", "HINDI", "BUSINESS STUDIES", "GEOGRAPHY", "SOCIAL SCIENCE", "HISTORY", "POLITICAL SCIENCE", "GENERAL KNOWLEDGE", "SOCIOLOGY"];
    const result = await getLangClassSubjectDetails();
    result.forEach((item) => {
        item.class = item.class.trim();
        item.package_language = item.package_language.trim().toLowerCase();
        item.subject = item.subject.trim();
    });
    const languagesMapping = _.groupBy(result, "package_language");
    const languageMappingArr = Object.keys(languagesMapping);
    let i = 0;
    redisClient.setAsync("WEB_BREADCRUMBS_LANGUAGES", JSON.stringify(languageMappingArr), "Ex", 60 * 60 * 24 * 7 * 2);
    for (const languageKey in languagesMapping) {
        if (languagesMapping[languageKey]) {
            const classesMapping = _.groupBy(languagesMapping[languageKey], "class");
            redisClient.setAsync(`WEB_BREADCRUMBS_LANGUAGE_CLASSES:${languageKey}`, JSON.stringify(Object.keys(classesMapping)), "Ex", 60 * 60 * 24 * 7 * 2);
            for (const classesKey in classesMapping) {
                if (classesMapping[classesKey]) {
                    const subjectMapping = _.groupBy(classesMapping[classesKey], "subject");
                    const subjectsArr = Object.keys(subjectMapping);
                    const subjectsFinal = subjectsArr.filter((item) => allowedSubjects.includes(item));

                    redisClient.setAsync(`WEB_BREADCRUMBS_CLASSES_SUBJECTS:${languageKey}:${classesKey}`, JSON.stringify(subjectsFinal), "Ex", 60 * 60 * 24 * 7 * 2);
                    for (const subjectKey in subjectMapping) {
                        if (subjectMapping[subjectKey]) {
                            let questionLevelDetails = await getWebQNAVideoDetails(languageKey, classesKey, subjectKey);
                            questionLevelDetails = questionLevelDetails.filter((item) => !_.isNull(item.chapter) && !_.isNull(item.subject) && !_.isNull(item.class));
                            questionLevelDetails.forEach((item) => {
                                item.class = item.class.trim();
                                item.subject = item.subject.trim().toLowerCase();
                                item.chapter = item.chapter.trim().toLowerCase();
                                item.package_language = item.package_language.trim().toLowerCase();
                                item.is_chapter_trans_present = !!item.chapter_trans;
                                item.chapter_trans = item.chapter_trans ? item.chapter_trans.trim().toLowerCase() : item.chapter;
                            });
                            const chapterMapping = _.groupBy(questionLevelDetails, "chapter");
                            const chapterTransMapping = _.groupBy(questionLevelDetails, "chapter_trans");
                            const chaptersObject = questionLevelDetails.reduce((acc, item) => {
                                const index = acc.findIndex((item2) => item2.chapter_trans === item.chapter_trans);
                                if (index < 0) {
                                    acc.push({
                                        chapter_trans: item.chapter_trans,
                                        chapter: item.chapter,
                                        is_chapter_trans_present: item.is_chapter_trans_present,
                                    });
                                }
                                return acc;
                            }, []);
                            redisClient.setAsync(`WEB_BREADCRUMBS_SUBJECT_CHAPTERS:${languageKey}:${classesKey}:${subjectKey.toLowerCase()}`, JSON.stringify(chaptersObject), "Ex", 60 * 60 * 24 * 7 * 2);
                            for (const chapterKey in chapterTransMapping) {
                                if (chapterTransMapping[chapterKey]) {
                                    const questionMapping = _.groupBy(chapterTransMapping[chapterKey], "question_id");
                                    let questionIds = Object.keys(questionMapping);
                                    questionIds = questionIds.splice(0, 50);
                                    redisClient.setAsync(`WEB_BREADCRUMBS_CHAPTER_TRANS_VIDEOS:${languageKey}:${classesKey}:${subjectKey.toLowerCase()}:${chapterKey.toLowerCase().trim()}`, JSON.stringify({
                                        questionIds,
                                        size: questionIds.length,
                                    }), "Ex", 60 * 60 * 24 * 7 * 2);
                                }
                            }
                            for (const chapterKey in chapterMapping) {
                                if (chapterMapping[chapterKey]) {
                                    const questionMapping = _.groupBy(chapterMapping[chapterKey], "question_id");
                                    let questionIds = Object.keys(questionMapping);
                                    questionIds = questionIds.splice(0, 50);
                                    redisClient.setAsync(`WEB_BREADCRUMBS_CHAPTER_VIDEOS:${languageKey}:${classesKey}:${subjectKey.toLowerCase()}:${chapterKey.toLowerCase().trim()}`, JSON.stringify({
                                        questionIds,
                                        size: questionIds.length,
                                    }), "Ex", 60 * 60 * 24 * 7 * 2);
                                }
                            }
                        }
                    }
                }
            }
            job.progress(parseInt(((i + 1) / languageMappingArr.length) * 100));
            i++;
        }
    }
    job.progress(100);
    return {
        data: {
            done: true,
        },
    };
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 3 * * *", // * 3 AM Everyday
};
