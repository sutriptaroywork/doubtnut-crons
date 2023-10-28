/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const moment = require("moment");
const { mysql, redis } = require("../../modules/index");
const { redshift } = require("../../modules");

async function getCourses() {
    const sql = "select distinct(assortment_id) from course_details where assortment_type = 'course' and is_free = 0";
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getAllTestIdsByCourse(CourseId) { // 45 ms
    const sql = `select * from testseries where course_id = ${CourseId} and is_active = 1`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getAllTestScores(TestIds) {
    const sql = `select student_id,test_id,totalscore,totalmarks,created_at from classzoo1.testseries_student_reportcards where test_id in (${TestIds}) and student_id not in (select student_id from classzoo1.internal_subscription)`;
    const users = await redshift.query(sql).then((res) => res);
    return users;
}

async function setCourseLeaderboardAll(assortmentId, marks, studentID) {
    return redis
        .multi()
        .zadd(`leaderboard:tests:all:${assortmentId}`, marks, studentID)
        .expire(`leaderboard:tests:all:${assortmentId}`, 60 * 60 * 24 * 3)
        .execAsync();
}

async function setCourseLeaderboardWeekly(assortmentId, marks, studentID) {
    return redis
        .multi()
        .zadd(`leaderboard:tests:weekly:${assortmentId}`, marks, studentID)
        .expire(`leaderboard:tests:weekly:${assortmentId}`, 60 * 60 * 24 * 3)
        .execAsync();
}

async function setCourseLeaderboardMonthly(assortmentId, marks, studentID) {
    return redis
        .multi()
        .zadd(`leaderboard:tests:monthly:${assortmentId}`, marks, studentID)
        .expire(`leaderboard:tests:monthly:${assortmentId}`, 60 * 60 * 24 * 3)
        .execAsync();
}

async function delCourseLeaderboardAll(assortmentId) {
    return redis.delAsync(`leaderboard:tests:all:${assortmentId}`);
}

async function delCourseLeaderboardWeekly(assortmentId) {
    return redis.delAsync(`leaderboard:tests:weekly:${assortmentId}`);
}

async function delCourseLeaderboardMonthly(assortmentId) {
    return redis.delAsync(`leaderboard:tests:monthly:${assortmentId}`);
}

async function getUserPackages(studentID) { // 150 ms
    const sql = `select student_id ,cd.assortment_id, batch_id from (select * from classzoo1.student_package_subscription where student_id in (${studentID}) and ((start_date < now() and end_date > now() and is_active=1) or (amount>-1 and end_date < now())) order by id desc) as a inner join (select * from classzoo1.package where reference_type in ('v3', 'onlyPanel', 'default')) as b on a.new_package_id=b.id left join (select class,assortment_id, assortment_type,display_name, year_exam,display_description,category,meta_info from classzoo1.course_details) as cd on cd.assortment_id=b.assortment_id order by a.id desc`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getBatchByAssortmentIdAndStudentId(studentIDs, assortmentID) {
    const batch = [];
    if (assortmentID) {
        const checkPurchaseHistoryAll = await getUserPackages(studentIDs);
        const checkPurchaseHistoryFiltered = checkPurchaseHistoryAll.filter((v, i, a) => a.findIndex((t) => (JSON.stringify(t) === JSON.stringify(v))) === i);
        for (let i = 0; i < studentIDs.length; i++) {
            const checkPurchaseHistory = checkPurchaseHistoryFiltered.filter((e) => e.student_id == studentIDs[i]);
            const currentAssortmentPurchaseHistory = _.find(checkPurchaseHistory, ["assortment_id", +assortmentID]);
            if (currentAssortmentPurchaseHistory) {
                batch.push(currentAssortmentPurchaseHistory.batch_id);
            } else {
                batch.push(1);
            }
        }
    }
    return batch;
}

async function start(job) {
    try {
        job.progress(0);
        const courseAssortments = await getCourses();
        if (courseAssortments && courseAssortments[0]) {
            for (let i = 0; i < courseAssortments.length; i++) {
                // eslint-disable-next-line no-await-in-loop
                const allTestIdByCourse = await getAllTestIdsByCourse(courseAssortments[i].assortment_id);
                const testIds = [];
                allTestIdByCourse.forEach((element) => {
                    testIds.push(element.test_id);
                });
                let allTestScores;
                if (testIds.length) {
                    // eslint-disable-next-line no-await-in-loop
                    allTestScores = await getAllTestScores(testIds);
                } else {
                    allTestScores = [];
                }
                const allTestScoresFinal = [];
                for (let j = 0; j < allTestScores.length; j++) {
                    // eslint-disable-next-line no-loop-func
                    const index = _.findIndex(allTestIdByCourse, (o) => o.test_id == allTestScores[j].test_id);
                    if (index > -1) {
                        // adding 5 hours and 30 minutes to exam time as this seems to be in UTC
                        if (moment(allTestIdByCourse[index].publish_time).isBefore(moment(allTestScores[j].created_at).add(5, "hours").add(30, "minutes"))) {
                            allTestScoresFinal.push(allTestScores[j]);
                        }
                    }
                }
                allTestScores = allTestScoresFinal;
                const timeNowToday = moment().add(5, "hours").add(30, "minutes");
                const timeNowToday2 = moment().add(5, "hours").add(30, "minutes");
                const sevenDayStart = timeNowToday.startOf("week");
                const monthStart = timeNowToday2.startOf("month");
                const weeklyTestScores = [];
                const monthlyTestScores = [];
                if (allTestScores && allTestScores[0]) {
                    for (let j = 0; j < allTestScores.length; j++) {
                        const examTime = moment(allTestScores[j].created_at).subtract(5, "hours").subtract(30, "minutes").format();
                        if (sevenDayStart.isBefore(examTime)) {
                            weeklyTestScores.push(allTestScores[j]);
                        }
                        if (monthStart.isBefore(examTime)) {
                            monthlyTestScores.push(allTestScores[j]);
                        }
                    }
                }
                const uniqueAllStudent = [...new Set(allTestScores.map((item) => item.student_id))];
                const uniqueWeeklyStudent = [...new Set(weeklyTestScores.map((item) => item.student_id))];
                const uniqueMonthlyStudent = [...new Set(monthlyTestScores.map((item) => item.student_id))];
                let batchIDAll;
                const batchIDWeekly = [];
                const batchIDMonthly = [];
                if (uniqueAllStudent && uniqueAllStudent.length) {
                    // eslint-disable-next-line no-await-in-loop
                    batchIDAll = await getBatchByAssortmentIdAndStudentId(uniqueAllStudent, courseAssortments[i].assortment_id);
                }
                if (uniqueWeeklyStudent && uniqueWeeklyStudent.length) {
                    for (let j = 0; j < uniqueWeeklyStudent.length; j++) {
                        const index = uniqueAllStudent.indexOf(uniqueWeeklyStudent[j]);
                        const batchStudent = batchIDAll[index];
                        batchIDWeekly.push(batchStudent);
                    }
                }
                if (uniqueMonthlyStudent && uniqueMonthlyStudent.length) {
                    // eslint-disable-next-line no-await-in-loop
                    for (let j = 0; j < uniqueMonthlyStudent.length; j++) {
                        const index = uniqueAllStudent.indexOf(uniqueMonthlyStudent[j]);
                        const batchStudent = batchIDAll[index];
                        batchIDMonthly.push(batchStudent);
                    }
                }
                const uniqueBatches = [...new Set(batchIDAll)];
                // delete redis keys
                for (let j = 0; j < uniqueBatches.length; j++) {
                    const id = `${courseAssortments[i].assortment_id}_${uniqueBatches[j]}`;
                    // eslint-disable-next-line no-await-in-loop
                    await delCourseLeaderboardAll(id);
                    // eslint-disable-next-line no-await-in-loop
                    await delCourseLeaderboardWeekly(id);
                    // eslint-disable-next-line no-await-in-loop
                    await delCourseLeaderboardMonthly(id);
                }
                for (let j = 0; j < uniqueAllStudent.length; j++) {
                    const newId = `${courseAssortments[i].assortment_id}_${batchIDAll[j]}`;
                    const allScoreOfStudent = allTestScores.filter((item) => item.student_id == uniqueAllStudent[j]);
                    let studentScore = 0;
                    let studentHasScore = false;
                    if (allScoreOfStudent && allScoreOfStudent[0]) {
                        for (let k = 0; k < allScoreOfStudent.length; k++) {
                            studentScore += parseInt(allScoreOfStudent[k].totalscore);
                            studentHasScore = true;
                        }
                    }
                    if (studentHasScore) {
                        await setCourseLeaderboardAll(newId, studentScore, uniqueAllStudent[j]);
                    }
                }
                for (let j = 0; j < uniqueWeeklyStudent.length; j++) {
                    const newId = `${courseAssortments[i].assortment_id}_${batchIDWeekly[j]}`;
                    const allScoreOfStudent = weeklyTestScores.filter((item) => item.student_id == uniqueWeeklyStudent[j]);
                    let studentScore = 0;
                    let studentHasScore = false;
                    if (allScoreOfStudent && allScoreOfStudent[0]) {
                        for (let k = 0; k < allScoreOfStudent.length; k++) {
                            studentScore += parseInt(allScoreOfStudent[k].totalscore);
                            studentHasScore = true;
                        }
                    }
                    if (studentHasScore) {
                        await setCourseLeaderboardWeekly(newId, studentScore, uniqueWeeklyStudent[j]);
                    }
                }
                for (let j = 0; j < uniqueMonthlyStudent.length; j++) {
                    const newId = `${courseAssortments[i].assortment_id}_${batchIDMonthly[j]}`;
                    const allScoreOfStudent = monthlyTestScores.filter((item) => item.student_id == uniqueMonthlyStudent[j]);
                    let studentScore = 0;
                    let studentHasScore = false;
                    if (allScoreOfStudent && allScoreOfStudent[0]) {
                        for (let k = 0; k < allScoreOfStudent.length; k++) {
                            studentScore += parseInt(allScoreOfStudent[k].totalscore);
                            studentHasScore = true;
                        }
                    }
                    if (studentHasScore) {
                        await setCourseLeaderboardMonthly(newId, studentScore, uniqueMonthlyStudent[j]);
                    }
                }
                const newLocal = (i + 1) * 100;
                console.log(newLocal);
                // eslint-disable-next-line no-await-in-loop
                await job.progress(parseInt((newLocal) / courseAssortments.length));
            }
        }
        // job.progress(100);
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
    cron: "30 2 * * *", // * 2:30 at night everyday
};
