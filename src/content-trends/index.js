/* eslint-disable no-await-in-loop */
const { redshift, mysql } = require("../../modules");

function getTopChapter(setClass, setSubject) {
    const sql = `SELECT c.class, d.subject, c.chapter, c.date_v, count(c.view_id) as count_v from (Select date(a.created_at) as date_v,a.view_id, b.class, b.chapter, b.subtopic from (SELECT * FROM classzoo1.video_view_stats where created_at + interval '330 minutes'>=current_date - 1 and created_at + interval '330 minutes' < current_date and source like 'android' and view_from like 'SRP') as a left join  classzoo1.questions_meta as b on a.question_id=b.question_id  where b.chapter is not NULL and b.class = '${setClass}') as c  left join (Select DISTINCT class, subject, chapter from classzoo1.mc_course_mapping) as d on c.class = d.class and c.chapter=d.chapter where upper(d.subject) like '${setSubject}' group by c.date_v, c.class, d.subject, c.chapter order by count_v desc limit 5`;
    // console.log(sql);
    return redshift.query(sql).then((res) => res);
}

function getSubTopic(setClass, chapter) {
    const sql = `SELECT c.class, c.subtopic, c.chapter, c.date_v, count(c.view_id) as count_st from (Select date(a.created_at) as date_v,a.view_id, b.class, b.chapter, b.subtopic from (SELECT * FROM classzoo1.video_view_stats where created_at + interval '330 minutes'>=current_date - 1 and created_at + interval '330 minutes' < current_date  and source like 'android' and view_from like 'SRP') as a left join  classzoo1.questions_meta as b on a.question_id=b.question_id  where b.chapter like '${chapter}' and b.class = '${setClass}') as c group by c.date_v, c.class, c.subtopic, c.chapter order by count_st desc limit 5`;
    // console.log(sql);
    return redshift.query(sql).then((res) => res);
}

function getLocalisedSubtopic(stClass, subject, engSubtopic) {
    const sql = "select subtopic_hindi from localized_mc_course_mapping where class=? and subject=? and subtopic=? limit 1";
    // console.log(sql);
    return mysql.pool.query(sql, [stClass, subject, engSubtopic]).then(([res]) => res);
}

function insertContentTrend(obj) {
    const sql = "INSERT INTO content_trend set ?";
    return mysql.writePool.query(sql, obj).then(([res]) => res);
}

function addSlashes(str) {
    str = str.replace(/\\/g, "\\\\").replace(/"/g, "\\'").replace(/'/g, "\\'");
    return str;
}

async function getTrendingTopics() {
    const sClass = [6, 7, 8, 9, 10, 11, 12, 14];
    const subject = ["PHYSICS", "CHEMISTRY", "MATHS", "BIOLOGY"];
    const promise = [];
    for (let j = 0; j < sClass.length; j++) {
        const setClass = sClass[j];
        for (let k = 0; k < subject.length; k++) {
            const setSubject = subject[k];
            const getTopChapterData = await getTopChapter(setClass, setSubject);
            for (let l = 0; l < getTopChapterData.length; l++) {
                const dataClass = getTopChapterData[l].class;
                const chapter = getTopChapterData[l].chapter ? addSlashes(getTopChapterData[l].chapter) : null;
                console.log(`${getTopChapterData[l].date_v} class ${dataClass}`);
                if (chapter) {
                    const subtopicData = await getSubTopic(dataClass, chapter);
                    for (let m = 0; m < subtopicData.length; m++) {
                        const obj = {};
                        obj.date_v = getTopChapterData[l].date_v;
                        obj.class = dataClass;
                        obj.subject = getTopChapterData[l].subject;
                        obj.chapter = chapter;
                        obj.subtopic = subtopicData[m].subtopic ? addSlashes(subtopicData[m].subtopic) : null;
                        obj.count_chapter = getTopChapterData[l].count_v;
                        obj.count_subtopic = subtopicData[m].count_st;
                        if (obj.subtopic) {
                            const hindiSubtopicSqlData = await getLocalisedSubtopic(obj.class, obj.subject, obj.subtopic);
                            // console.log(hindiSubtopicSqlData);
                            obj.hindi_subtopic = (hindiSubtopicSqlData && hindiSubtopicSqlData.length && hindiSubtopicSqlData[0].subtopic_hindi) ? hindiSubtopicSqlData[0].subtopic_hindi : null;
                            promise.push(insertContentTrend(obj));
                        }
                    }
                }
            }
        }
    }
    await Promise.all(promise);
}

async function start(job) {
    await getTrendingTopics();
    await job.progress(100);
    console.log(`the script successfully ran at ${new Date()}`);
    return { data: "success" };
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 1 * * *",
};
