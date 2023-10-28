/* eslint-disable no-await-in-loop */
const moment = require("moment");
const redis = require("../../modules/redis");
const { mysql, config, kafka } = require("../../modules/index");

async function getPaidAssortmentIdList() {
    const sql = "SELECT DISTINCT assortment_id from course_details cd where (is_active or is_active_sales) and is_free = 0 and assortment_type  = 'course'";
    return mysql.pool.query(sql).then((x) => x[0]);
}

async function getStudentDataAndPackageDuration(studentId, assortmentId) {
    const sql = "select s.*,duration_in_days, p.batch_id, p.assortment_id from (SELECT * from student_package_subscription sps where NOW()>start_date and NOW()< end_date and is_active and student_id = ?) as a JOIN (select * from students where student_id = ?) as s on a.student_id =s.student_id join package p  on a.new_package_id = p.id WHERE assortment_id = ? limit 1";// 100 ms
    return mysql.pool.query(sql, [studentId, studentId, assortmentId]);
}

async function getStudentsWithScoreLessThanFifty(lastMonthNumber, assortmentId) {
    return redis.zrangebyscoreAsync(`padho_aur_jeeto_monthly_leaderboard:${lastMonthNumber}:${assortmentId}`, 0, 50);
}

async function getStudentsWithScoreMoreThanNinety(lastMonthNumber, assortmentId) {
    return redis.zrangebyscoreAsync(`padho_aur_jeeto_monthly_leaderboard:${lastMonthNumber}:${assortmentId}`, 90, "inf");
}
const notificationTemplateMoreThanNinety = {
    notification: {
        title: "Padho aur jeeto",
        firebaseTag: "PADHO_AUR_JEETO_SCORE_MORE_THAN_NINETY",
        image_url: `${config.staticCDN}engagement_framework/46087958-D903-BA89-0110-B297292748D5.webp`,
    },
};

const notificationTemplateLessThanFifty = {
    notification: {
        title: "Padho aur jeeto",
        firebaseTag: "PADHO_AUR_JEETO_SCORE_LESS_THAN_FIFTY",
        image_url: `${config.staticCDN}engagement_framework/6758B9AF-3366-2DFD-4F95-32D24191264C.webp`,
    },
};

async function sendNotification(students, notificationTemplate, assortmentId) {
    for (let i = 0; i < students.length; i++) {
        try {
            const row = students[i][0][0];
            console.log(row);
            const notificationPayload = {
                event: "course_details",
                image: notificationTemplate.notification.image_url,
                title: notificationTemplate.notification.title,
                message: "Padho aur Jeeto",
                firebase_eventtag: notificationTemplate.notification.firebaseTag,
                s_n_id: notificationTemplate.notification.firebaseTag,
            };
            if (row.gcm_reg_id) {
                notificationPayload.data = JSON.stringify({
                    id: assortmentId,
                });

                await kafka.sendNotification(
                    [row.student_id], [row.gcm_reg_id],
                    notificationPayload,
                );
            }
        } catch (err) {
            console.log(err);
        }
    }
}

async function start(job) {
    try {
        const now = moment().add(5, "hours").add(30, "minutes");
        const monthNumber = now.month();
        const assortmentIdResp = await getPaidAssortmentIdList();
        const assortmentIdList = [];
        assortmentIdResp.forEach((item) => assortmentIdList.push(item.assortment_id));
        // console.log({assortmentIdList, assortmentIdResp});
        for (let i = 0; i < assortmentIdList.length; i++) {
            const assortmentId = assortmentIdList[i];
            const studentsWithScoreLessThanFifty = await getStudentsWithScoreLessThanFifty(monthNumber, assortmentId);
            console.log({ studentsWithScoreLessThanFifty });

            const studentDataPromises = [];
            for (let j = 0; j < studentsWithScoreLessThanFifty.length; j++) {
                studentDataPromises.push(getStudentDataAndPackageDuration(studentsWithScoreLessThanFifty[j], assortmentId));
            }

            const studentDataArr = await Promise.all(studentDataPromises);

            sendNotification(studentDataArr, notificationTemplateLessThanFifty, assortmentId);

            const studentsWithScoreMoreThanNinety = await getStudentsWithScoreMoreThanNinety(monthNumber, assortmentId);
            const studentDataPromises2 = [];
            for (let j = 0; j < studentsWithScoreMoreThanNinety.length; j++) {
                studentDataPromises2.push(getStudentDataAndPackageDuration(studentsWithScoreMoreThanNinety[j], assortmentId));
            }
            const studentDataArr2 = await Promise.all(studentDataPromises2);
            sendNotification(studentDataArr2, notificationTemplateMoreThanNinety, assortmentId);
        }
    } catch (e) {
        console.error(e);
    }
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "00 12 */3 * *", // At 12:00 every third day
};
