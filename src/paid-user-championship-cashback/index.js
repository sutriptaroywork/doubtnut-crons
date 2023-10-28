/* eslint-disable no-await-in-loop */
const moment = require("moment");
const axios = require("axios");
const redis = require("../../modules/redis");
const { mysql, kafka, config } = require("../../modules/index");

async function getPaidUserChampionshipLeaderboardAnnual(year, assortmentId, min, max) {
    return redis.zrevrange(`padho_aur_jeeto_annual_leaderboard:${year}:${assortmentId}`, min, max);
}
async function getPaidUserChampionshipLeaderboardMonthly(monthNumber, assortmentId, min, max) {
    return redis.zrevrange(`padho_aur_jeeto_monthly_leaderboard:${monthNumber}:${assortmentId}`, min, max);
}

async function getPaidUserChampionshipLeaderboardAnnualScore(year, assortmentId, studentId) {
    return redis.zscore(`padho_aur_jeeto_annual_leaderboard:${year}:${assortmentId}`, studentId);
}
async function getPaidUserChampionshipLeaderboardMonthlyScore(monthNumber, assortmentId, studentId) {
    return redis.zscore(`padho_aur_jeeto_monthly_leaderboard:${monthNumber}:${assortmentId}`, studentId);
}
const notificationTemplate = {
    notification: {
        title: "Padho aur jeeto",
        firebaseTag: "PADHO_AUR_JEETO_WIN_NOTIFICATION",
        image_url: `${config.staticCDN}engagement_framework/78C65688-57B7-25E8-3713-2DE02D4BD279.webp`,
    },
};

async function getStudentDetails(studentId) {
    const sql = "select * from students where student_id = ?";
    return mysql.pool.query(sql, [studentId]).then((x) => x[0][0]);
}

async function sendNotification(student, amount) {
    const row = await getStudentDetails(student);
    const notificationPayload = {
        event: "wallet",
        image: notificationTemplate.notification.image_url,
        title: notificationTemplate.notification.title,
        message: `You have won ₹${amount}. From padho aur jeeto contest.`,
        firebase_eventtag: notificationTemplate.notification.firebaseTag,
        s_n_id: notificationTemplate.notification.firebaseTag,
    };
    if (row.gcm_reg_id) {
        await kafka.sendNotification(
            [row.student_id], [row.gcm_reg_id],
            notificationPayload,
        );
    }
}

async function getPaidAssortmentIdList() {
    const sql = "SELECT DISTINCT assortment_id from course_details cd where NOW()>= start_date  and NOW()<= end_date  and is_free = 0 and assortment_type  = 'course'";
    return mysql.pool.query(sql).then((x) => x[0]);
}

async function getAmount(studentId, assortmentId) {
    const sql = "select amount,a.created_at from (SELECT * from student_package_subscription sps where student_id = ? ) as a JOIN package p  on a.new_package_id = p.id WHERE assortment_id = ? order by created_at desc limit 1";
    return mysql.pool.query(sql, [studentId, assortmentId]);
}

async function getCoursesThatEndedLastMonth(lastMonthNumber, year) {
    const sql = "SELECT DISTINCT assortment_id FROM course_details cd WHERE YEAR(end_date) = ? AND MONTH(end_date) = ? and assortment_type = 'course'";
    return mysql.pool.query(sql, [year, lastMonthNumber]);
}

async function insertTshirtWinners(studentId, winningDate, assortmentId, duration, rank, percentage) {
    const sql = "insert into paid_user_championship_shirt_winners (student_id, winning_date, assortment_id, duration, rank, percentage, reward) values (?,?,?,?,?,?,?) ";
    return mysql.writePool.query(sql, [studentId, winningDate, assortmentId, duration, rank, percentage, "T-Shirt"]);
}
async function makeWalletTransaction(walletData, timeout = 2000) {
    try {
        const headers = { "Content-Type": "application/json" };
        const { data } = await axios({
            method: "POST",
            url: "https://micro.doubtnut.com/wallet/transaction/create",
            timeout,
            headers,
            data: walletData,
        });
        return data;
    } catch (e) {
        console.error(e);
        return false;
    }
}
async function start(job) {
    try {
        const now = moment().add(5, "hours").add(30, "minutes");
        const lastMonthNumber = now.subtract(1, "months").month();
        const lastYear = now.subtract(1, "months").year();
        const assortmentIds = (await getCoursesThatEndedLastMonth(lastMonthNumber + 1, lastYear))[0];
        const paidAssortmentList = await getPaidAssortmentIdList();
        const assortmentList = [];
        paidAssortmentList.forEach((item) => assortmentList.push(item.assortment_id));
        for (let i = 0; i < assortmentList.length; i++) {
            const assortmentId = assortmentList[i];
            const winners = await getPaidUserChampionshipLeaderboardMonthly(lastMonthNumber, assortmentId, 0, 2);
            for (let j = 0; j < winners.length; j++) {
                let amountPaid = (await getAmount(winners[j], assortmentId))[0][0];
                console.log(amountPaid);

                if (amountPaid) {
                    amountPaid = amountPaid.amount;
                } else {
                    amountPaid = 0;
                }
                // const percentage = await getPaidUserChampionshipLeaderboardMonthlyScore(lastMonthNumber, assortmentId, winners[j]);
                // const winningDate = moment().startOf("month").format("YYYY-MM-DD HH:mm:ss");
                // await insertTshirtWinners(winners[j], winningDate, assortmentId, "monthly", j + 1, percentage);
                if (amountPaid > 0) {
                    sendNotification(winners[j], amountPaid * 0.025);
                    makeWalletTransaction({
                        student_id: parseInt(winners[j]),
                        reward_amount: amountPaid * 0.025,
                        type: "CREDIT",
                        payment_info_id: "dedsorupiyadega",
                        reason: "padho_aur_jeeto_monthly_reward",
                    });
                }
            }
        }
        for (let i = 0; i < assortmentIds.length; i++) {
            const key = `padho_aur_jeeto_annual_leaderboard:${lastYear}:${assortmentIds[i].assortment_id}`;
            const splitKey = key.split(":");
            const assortmentId = splitKey[2];
            const winners = await getPaidUserChampionshipLeaderboardAnnual(lastYear, assortmentId, 0, 3);
            for (let j = 0; j < winners.length; j++) {
                let amountPaid = (await getAmount(winners[j], assortmentId))[0][0];
                if (amountPaid) {
                    amountPaid = amountPaid.amount;
                } else {
                    amountPaid = 0;
                }
                // const percentage = await getPaidUserChampionshipLeaderboardAnnualScore(lastYear, assortmentId, winners[j]);
                // const winningDate = moment().startOf("yearly").format("YYYY-MM-DD HH:mm:ss");
                // // await insertTshirtWinners(winners[j], winningDate, assortmentId, "yearly", j + 1, percentage);
                if (amountPaid > 0) {
                    sendNotification(winners[j], amountPaid * 0.25);
                    makeWalletTransaction({
                        student_id: parseInt(winners[j]),
                        reward_amount: amountPaid * 0.25,
                        type: "CREDIT",
                        payment_info_id: "dedsorupiyadega",
                        reason: "padho_aur_jeeto_annual_reward",
                    });
                }
            }
        }

        job.progress(90);
    } catch (e) {
        console.error(e);
    }
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 0 1 * *", // At 00:00 on first day of month
};
