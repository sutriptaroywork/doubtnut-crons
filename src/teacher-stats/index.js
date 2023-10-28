/* eslint-disable no-await-in-loop */
/* eslint-disable no-constant-condition */
const _ = require("lodash");
const moment = require("moment");
const gc = require("expose-gc/function");
const { mysql } = require("../../modules");
const { redis } = require("../../modules");

async function getVerifiedTeachers() {
    const sql = "select * from teachers where is_verified = 1 and is_active = 1";
    return mysql.pool.query(sql).then((res) => res[0]);
}

function getResourceByTeacher() {
    return "select cr.* from course_resources cr left join teachers_resource_upload tru on tru.course_resource_id = cr.id where tru.is_uploaded = 1";
}

async function getStatsFromTeacherStats(courseResourceId) {
    const sql = `select * from teachers_stats where course_resource_id in (${courseResourceId})`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getStatsFromPdfStats(resourceIds) {
    const sql = `select * from pdf_download_stats where resource_id in (${resourceIds})`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getVideoViewsByQuestionId(questionId, date) {
    let sql;
    if (date) {
        sql = `select question_id, count(*) as count from video_view_stats where question_id = ${questionId} and created_at > '${date}'`;
    } else {
        sql = `select question_id, count(*) as count from video_view_stats where question_id = ${questionId}`;
    }
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function insertTeacherStatsVideo(videoViews) {
    const sql = `insert into teachers_stats (course_resource_id, question_id, views) values (${videoViews.course_resource_id}, "${videoViews.question_id}", ${videoViews.count})`;
    return mysql.writePool.query(sql).then((res) => res);
}

async function updateTeacherStatsVideo(videoViews) {
    const sql = `update teachers_stats set views = ${videoViews.count} where course_resource_id = ${videoViews.course_resource_id}`;
    return mysql.writePool.query(sql).then((res) => res);
}

async function insertTeacherStatsPdf(pdfViews) {
    const sql = `insert into teachers_stats (course_resource_id, question_id, views) values (${pdfViews.course_resource_id}, "${pdfViews.resource_reference}", ${pdfViews.count})`;
    return mysql.writePool.query(sql).then((res) => res);
}

async function updateTeacherStatsPdf(pdfViews) {
    const sql = `update teachers_stats set views = ${pdfViews.count} where course_resource_id = ${pdfViews.course_resource_id}`;
    return mysql.writePool.query(sql).then((res) => res);
}

async function getVideoQIDByTeacherId(teacherId) {
    const sql = `select cr.resource_reference as qid from course_resources cr left join teachers_resource_upload tru on tru.course_resource_id = cr.id where tru.teacher_id = ${teacherId} and cr.resource_type = 1`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getStatsFromTeacherStatsByQID(qidList) {
    const sql = `select sum(views) as total_views from teachers_stats where question_id in (${qidList})`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getNewVideosByTeacherIdCount(teacherId) {
    const sql = `select count(*) as count from teachers_resource_upload tru inner join course_resources cr on tru.course_resource_id = cr.id inner join answers a on a.question_id = cr.resource_reference inner join answer_video_resources avr on avr.answer_id = a.answer_id where tru.teacher_id = ${teacherId} and tru.is_uploaded = 1 and cr.resource_type = 1 and cr.created_at > DATE_SUB(NOW(), INTERVAL 3 DAY)`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function start(job) {
    try {
        const verifiedTeachers = await getVerifiedTeachers();
        const teacherList = [];
        _.forEach(verifiedTeachers, (teacher) => {
            teacherList.push(teacher.teacher_id);
        });
        if (!_.isEmpty(teacherList)) {
            const resourceQuery = getResourceByTeacher();
            // divide teachers in chunks of 1000
            const chunk1 = 1000;
            for (let i = 0, j = teacherList.length; i < j; i += chunk1) {
                const slice1 = teacherList.slice(i, i + chunk1);
                let l = 1;
                while (true) {
                    gc();
                    // get resources for teacehers in the chunk by a 50000 limit
                    const sql1 = `${resourceQuery} and tru.teacher_id in (${slice1}) LIMIT 50000 OFFSET ${(l - 1) * 50000}`;
                    const resources = await mysql.pool.query(sql1).then((res) => res[0]);
                    if (resources.length === 0) {
                        break;
                    }
                    const videoResources = _.filter(resources, (resource) => resource.resource_type === 1);
                    const pdfResources = _.filter(resources, (resource) => resource.resource_type === 2);
                    // video handling
                    if (!_.isEmpty(videoResources)) {
                        // updating or inserting video views for each of the video resouces in 50000 chunks by dividing them in further chunk of 100
                        const chunk2 = 100;
                        for (let e = 0, f = videoResources.length; e < f; e += chunk2) {
                            const slice2 = videoResources.slice(e, e + chunk2);
                            const videoResourcesList = slice2.map((resource) => resource.id);
                            const getStats = await getStatsFromTeacherStats(videoResourcesList);
                            const workers1 = [];
                            // checking if resource is already in the table, if yes then fetching views after last update else fetching all views
                            for (let g = 0; g < slice2.length; g++) {
                                const index1 = getStats.map((stat) => stat.question_id).indexOf(slice2[g].resource_reference);
                                let date;
                                if (index1 !== -1) {
                                    const stat = getStats[index1];
                                    date = moment(stat.updated_at).format("YYYY-MM-DD HH:MM:SS");
                                }
                                workers1.push(getVideoViewsByQuestionId(slice2[g].resource_reference, date));
                            }
                            const videoViewsTemp = await Promise.all(workers1);
                            let videoViews = [];
                            _.forEach(videoViewsTemp, (videoView) => {
                                videoViews.push(videoView[0]);
                            });
                            videoViews = videoViews.filter((videoView) => videoView.count > 0);
                            for (let a = 0; a < videoViews.length; a++) {
                                const courseResourceIdIndex = videoResources.map((res) => res.resource_reference).indexOf(videoViews[a].question_id.toString());
                                if (courseResourceIdIndex !== -1) {
                                    const courseResourceId = videoResources[courseResourceIdIndex].id;
                                    videoViews[a].course_resource_id = courseResourceId;
                                }
                            }
                            // inserting or updating video views according to the presence in teachers_stats table
                            const workers2 = [];
                            for (let k = 0; k < videoViews.length; k += 1) {
                                const index1 = getStats.map((stat) => stat.question_id).indexOf(videoViews[k].question_id.toString());
                                if (index1 == -1) {
                                    workers2.push(insertTeacherStatsVideo(videoViews[k]));
                                } else {
                                    const countTemp = getStats[index1].views + videoViews[k].count;
                                    videoViews[k].count = countTemp;
                                    workers2.push(updateTeacherStatsVideo(videoViews[k]));
                                }
                            }
                            await Promise.all(workers2);
                        }
                    }
                    // pdf handling
                    if (!_.isEmpty(pdfResources)) {
                        // updating or inserting video views for each of the pdf resouces in 50000 chunks by dividing them in further chunk of 100
                        const chunk2 = 100;
                        for (let e = 0, f = pdfResources.length; e < f; e += chunk2) {
                            const slice2 = pdfResources.slice(e, e + chunk2);
                            const pdfResourcesList = slice2.map((resource) => resource.id);
                            const getPdfStats = await getStatsFromPdfStats(pdfResourcesList);
                            const getStats = await getStatsFromTeacherStats(pdfResourcesList);
                            const workers2 = [];
                            for (let g = 0; g < slice2.length; g++) {
                                const pdfRecords = getPdfStats.filter((pdf) => pdf.resource_id === slice2[g].id);
                                if (pdfRecords.length > 0) {
                                    const index1 = getStats.map((stat) => stat.course_resource_id).indexOf(slice2[g].id);
                                    const count = _.sumBy(pdfRecords, (pdf) => pdf.count);
                                    const pdfViews = {
                                        course_resource_id: slice2[g].id,
                                        resource_reference: slice2[g].resource_reference,
                                        count,
                                    };
                                    // inserting or updating pdf views according to the presence in teachers_stats table
                                    if (index1 == -1) {
                                        workers2.push(insertTeacherStatsPdf(pdfViews));
                                    } else {
                                        workers2.push(updateTeacherStatsPdf(pdfViews));
                                    }
                                }
                            }
                            await Promise.all(workers2);
                        }
                    }
                    l++;
                }
            }
            for (let i = 0; i < teacherList.length; i += 1) {
                const [qid, checkNewVideos] = await Promise.all([getVideoQIDByTeacherId(teacherList[i]), getNewVideosByTeacherIdCount(teacherList[i])]);
                const qidList = [];
                _.forEach(qid, (question) => {
                    qidList.push(question.qid);
                });
                let getStats = [];
                if (!_.isEmpty(qidList)) {
                    getStats = await getStatsFromTeacherStatsByQID(qidList);
                }
                if (!_.isEmpty(getStats) && getStats[0].total_views != null && getStats[0].total_views != 0) {
                    await redis.setAsync(`TEACHER:VIEWS:ALLTIME:${teacherList[i]}`, JSON.stringify(getStats[0].total_views), "Ex", 60 * 60 * 24 * 7);
                }
                if (!_.isEmpty(checkNewVideos) && checkNewVideos[0].count > 0) {
                    await redis.setAsync(`TEACHER:VIDEOS:NEW:${teacherList[i]}`, JSON.stringify(checkNewVideos[0].count), "Ex", 60 * 60 * 24 * 3); // 3 days, change if it is changed in the query
                }
            }
        }
        job.progress(100);
        return {
            data: {
                done: true,
            },
        };
    } catch (err) {
        console.log(err);
        return { err };
    }
}
module.exports.start = start;
module.exports.opts = {
    cron: "0 2 * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
