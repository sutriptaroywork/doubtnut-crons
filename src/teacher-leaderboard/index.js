/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const moment = require("moment");
// const axios = require("axios");
const { redis } = require("../../modules/index");
const { mysql } = require("../../modules");

async function getTeachersList() {
    const mysqlQ = "select * from teachers where is_verified = 1 and is_active = 1";
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}

// async function updateDefaultImage(teacherId) {
//     const mysqlQ = `update teachers set img_url = 'https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/E9F16D21-A0BC-C5AC-77D3-678C95588DFD.webp' where teacher_id = ${teacherId}`;
//     return mysql.writePool.query(mysqlQ).then(([res]) => res);
// }

async function setTeacherLeaderboard(type, views, teacherId) {
    return redis
        .multi()
        .zadd(`leaderboard:teachers:${type}`, views, teacherId)
        .expireat(`leaderboard:teachers:${type}`, parseInt((+new Date()) / 1000) + 60 * 60 * 24 * 3)
        .execAsync();
}

async function deleteTeacherLeaderboard(type) {
    return redis.delAsync(`leaderboard:teachers:${type}`);
}

async function getViewData(time, endtime) { // 10.4s
    let sql = `select
        t.teacher_id,
        count(distinct v.student_id) as students_who_viewed,
        count(view_id) as views,
        sum(case when (engage_time>5*video_time) then video_time else engage_time end) as "et"
        from teachers t
        left join (select id,vendor_id,resource_reference,resource_type,faculty_id from  course_resources where resource_type=1 group by 1,2,3,4,5) cr on cr.faculty_id=t.teacher_id
        left join teachers_resource_upload u on u.course_resource_id=cr.id
         join
        (select
                    vvs.question_id,
                    vvs.student_id,
                    created_at,
                    engage_time,video_time,
                    view_id
                    from video_view_stats vvs
                    left join questions q on q.question_id = vvs.question_id
                    where created_at >= '${time}'
                ) as v on v.question_id=cr.resource_reference
        where cr.resource_type=1 and t.is_verified=1 and cr.vendor_id=3
        group by 1
        order by 4 desc
        `;
    if (endtime) {
        sql = `select
        t.teacher_id,
        count(distinct v.student_id) as students_who_viewed,
        count(view_id) as views,
        sum(case when (engage_time>5*video_time) then video_time else engage_time end) as "et"
        from teachers t
        left join (select id,vendor_id,resource_reference,resource_type,faculty_id from  course_resources where resource_type=1 group by 1,2,3,4,5) cr on cr.faculty_id=t.teacher_id
        left join teachers_resource_upload u on u.course_resource_id=cr.id
         join
        (select
                    vvs.question_id,
                    vvs.student_id,
                    created_at,
                    engage_time,video_time,
                    view_id
                    from video_view_stats vvs
                    left join questions q on q.question_id = vvs.question_id
                    where created_at >= '${time}' and created_at < '${endtime}'
                ) as v on v.question_id=cr.resource_reference
        where cr.resource_type=1 and t.is_verified=1 and cr.vendor_id=3
        group by 1
        order by 4 desc
        `;
    }
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function start(job) {
    try {
        const verifiedTeachers = await getTeachersList();
        await Promise.all([deleteTeacherLeaderboard("daily"), deleteTeacherLeaderboard("weekly"), deleteTeacherLeaderboard("monthly")]);
        const teachersList = [];
        verifiedTeachers.forEach((e) => {
            teachersList.push(e.teacher_id);
        });
        const daily = moment().add(5, "hours").add(30, "minutes").subtract(10, "hours")
            .startOf("days")
            .format("YYYY-MM-DD");
        const weekly = moment().add(5, "hours").add(30, "minutes").subtract(7, "days")
            .startOf("week")
            .format("YYYY-MM-DD");
        const weeklyEndDate = moment().add(5, "hours").add(30, "minutes").startOf("week")
            .format("YYYY-MM-DD");
        const monthly = moment().add(5, "hours").add(30, "minutes").startOf("month")
            .format("YYYY-MM-DD");
        const dailyMaxViews = { et: 0, vv: 0, etbyvv: 0 };
        const weeklyMaxViews = { et: 0, vv: 0, etbyvv: 0 };
        const monthlyMaxViews = { et: 0, vv: 0, etbyvv: 0 };
        const [dailyViews, weeklyViews, monthlyViews] = await Promise.all([getViewData(daily, null), getViewData(weekly, weeklyEndDate), getViewData(monthly, null)]);
        const teachersWithViewDaily = new Set();
        const teachersWithViewWeekly = new Set();
        const teachersWithViewMonthly = new Set();

        // Finds the maximum et, vv, and et/vv in daily, weekly and monthly.
        for (let i = 0; i < dailyViews.length; i++) {
            dailyViews[i].et = +dailyViews[i].et;
            dailyMaxViews.et = dailyMaxViews.et < dailyViews[i].et ? dailyViews[i].et : dailyMaxViews.et;
            dailyMaxViews.vv = dailyMaxViews.vv < dailyViews[i].views ? dailyViews[i].views : dailyMaxViews.vv;
            dailyMaxViews.etbyvv = dailyMaxViews.etbyvv < (dailyViews[i].et / dailyViews[i].views) ? (dailyViews[i].et / dailyViews[i].views) : dailyMaxViews.etbyvv;
            // Keeps track of teachers with values to assign 0 as default for the rest
            teachersWithViewDaily.add(dailyViews[i].teacher_id);
        }
        for (let i = 0; i < weeklyViews.length; i++) {
            weeklyViews[i].et = +weeklyViews[i].et;
            weeklyMaxViews.et = weeklyMaxViews.et < weeklyViews[i].et ? weeklyViews[i].et : weeklyMaxViews.et;
            weeklyMaxViews.vv = weeklyMaxViews.vv < weeklyViews[i].views ? weeklyViews[i].views : weeklyMaxViews.vv;
            weeklyMaxViews.etbyvv = weeklyMaxViews.etbyvv < (weeklyViews[i].et / weeklyViews[i].views) ? (weeklyViews[i].et / weeklyViews[i].views) : weeklyMaxViews.etbyvv;
            teachersWithViewWeekly.add(weeklyViews[i].teacher_id);
        }
        for (let i = 0; i < monthlyViews.length; i++) {
            monthlyViews[i].et = +monthlyViews[i].et;
            monthlyMaxViews.et = monthlyMaxViews.et < monthlyViews[i].et ? monthlyViews[i].et : monthlyMaxViews.et;
            monthlyMaxViews.vv = monthlyMaxViews.vv < monthlyViews[i].views ? monthlyViews[i].views : monthlyMaxViews.vv;
            monthlyMaxViews.etbyvv = monthlyMaxViews.etbyvv < (monthlyViews[i].et / monthlyViews[i].views) ? (monthlyViews[i].et / monthlyViews[i].views) : monthlyMaxViews.etbyvv;
            teachersWithViewMonthly.add(monthlyViews[i].teacher_id);
        }
        let dailyMaxScore = 0;
        let weeklyMaxScore = 0;
        let monthlyMaxScore = 0;

        // Finds maximum value of final normalized score, obtained by value[i]*100/max(value)
        for (let i = 0; i < dailyViews.length; i++) {
            const dailyValue = ((0.3 * (+dailyViews[i].et * 100)) / dailyMaxViews.et) + ((0.2 * (dailyViews[i].views * 100)) / dailyMaxViews.vv) + ((0.5 * (dailyViews[i].et / dailyViews[i].views) * 100) / dailyMaxViews.etbyvv);
            if (dailyValue > dailyMaxScore) {
                dailyMaxScore = dailyValue;
            }
        }

        for (let i = 0; i < weeklyViews.length; i++) {
            const weeklyValue = ((0.3 * (+weeklyViews[i].et * 100)) / weeklyMaxViews.et) + ((0.2 * (weeklyViews[i].views * 100)) / weeklyMaxViews.vv) + ((0.5 * (weeklyViews[i].et / weeklyViews[i].views) * 100) / weeklyMaxViews.etbyvv);
            if (weeklyValue > weeklyMaxScore) {
                weeklyMaxScore = weeklyValue;
            }
        }

        for (let i = 0; i < monthlyViews.length; i++) {
            const monthlyValue = ((0.3 * (+monthlyViews[i].et * 100)) / monthlyMaxViews.et) + ((0.2 * (monthlyViews[i].views * 100)) / monthlyMaxViews.vv) + ((0.5 * (monthlyViews[i].et / monthlyViews[i].views) * 100) / monthlyMaxViews.etbyvv);
            if (monthlyValue > monthlyMaxScore) {
                monthlyMaxScore = monthlyValue;
            }
        }

        // Sets value upto 2 decimal points with same noramlized approach
        for (let i = 0; i < dailyViews.length; i++) {
            const dailyValue = ((0.3 * (+dailyViews[i].et * 100)) / dailyMaxViews.et) + ((0.2 * (dailyViews[i].views * 100)) / dailyMaxViews.vv) + ((0.5 * (dailyViews[i].et / dailyViews[i].views) * 100) / dailyMaxViews.etbyvv);
            await setTeacherLeaderboard("daily", +((dailyValue * 100) / dailyMaxScore).toFixed(2), dailyViews[i].teacher_id);
        }

        for (let i = 0; i < weeklyViews.length; i++) {
            const weeklyValue = ((0.3 * (+weeklyViews[i].et * 100)) / weeklyMaxViews.et) + ((0.2 * (weeklyViews[i].views * 100)) / weeklyMaxViews.vv) + ((0.5 * (weeklyViews[i].et / weeklyViews[i].views) * 100) / weeklyMaxViews.etbyvv);
            await setTeacherLeaderboard("weekly", +((weeklyValue * 100) / weeklyMaxScore).toFixed(2), weeklyViews[i].teacher_id);
        }

        for (let i = 0; i < monthlyViews.length; i++) {
            const monthlyValue = ((0.3 * (+monthlyViews[i].et * 100)) / monthlyMaxViews.et) + ((0.2 * (monthlyViews[i].views * 100)) / monthlyMaxViews.vv) + ((0.5 * (monthlyViews[i].et / monthlyViews[i].views) * 100) / monthlyMaxViews.etbyvv);
            console.log(+((monthlyValue * 100) / monthlyMaxScore).toFixed(2));
            await setTeacherLeaderboard("monthly", +((monthlyValue * 100) / monthlyMaxScore).toFixed(2), monthlyViews[i].teacher_id);
        }

        for (let i = 0; i < teachersList; i++) {
            if (!teachersWithViewDaily.has(teachersList[i])) {
                await setTeacherLeaderboard("daily", 0, teachersList[i]);
            }
            if (!teachersWithViewWeekly.has(teachersList[i])) {
                await setTeacherLeaderboard("weekly", 0, teachersList[i]);
            }
            if (!teachersWithViewMonthly.has(teachersList[i])) {
                await setTeacherLeaderboard("monthly", 0, teachersList[i]);
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
        return {
            err,
        };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 1 * * *", // * 1:00 at night everyday
    removeOnComplete: 10,
    removeOnFail: 10,
};
