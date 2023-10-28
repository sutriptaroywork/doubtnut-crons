const moment = require("moment");
const _ = require("lodash");
const axios = require("axios");
const redisClient = require("../../modules/redis");
const { getMongoClient } = require("../../modules");

const db = "doubtnut";
const leaderboardCollection = "khelo_jeeto_daily_leaderboard";

const DAILY_PRIZE = [15, 10, 5];

async function getLeaderboardList() {
    // To fetch previous day's leaderboard list - we are doing day() - 1
    return redisClient.zrevrangeAsync(`KJ_DAILY_LEADERBOARD_${moment().add(5, "hours").add(30, "minutes").date() - 1}`, 0, -1, "WITHSCORES");
}

async function delPreviousDayLeaderboardList() {
    // deleting old redis data after storing it in mongo
    return redisClient.delAsync(`KJ_DAILY_LEADERBOARD_${moment().add(5, "hours").add(30, "minutes").date() - 1}`);
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
        job.progress(10);
        console.log("task started");
        const client = (await getMongoClient()).db(db);
        const leaderboardList = await getLeaderboardList();
        job.progress(30);
        const studentIds = []; const studentWins = []; const studentRanks = [];
        let rankCounter = 1;
        // Structure data from redis to arrays
        for (let i = 0; i < leaderboardList.length; i++) {
            // Even index contains student_id and odd index contains score
            if (i % 2 === 0) {
                studentIds.push(leaderboardList[i]);
                studentRanks.push(rankCounter);
                rankCounter++;
            } else {
                studentWins.push(leaderboardList[i]);
            }
        }
        job.progress(50);
        if (studentIds.length) {
            // Give Rewards to Top 3 students
            const minLength = studentIds.length < 3 ? studentIds.length : 3;
            for (let i = 0; i < minLength; i++) {
                console.log(`${studentIds[i]} got ₹${DAILY_PRIZE[i]}`);
                makeWalletTransaction({
                    student_id: parseInt(studentIds[i]),
                    reward_amount: DAILY_PRIZE[i],
                    type: "CREDIT",
                    payment_info_id: "dedsorupiyadega",
                    reason: "khelo_jeeto_reward",
                });
            }
            job.progress(60);
            const leaderboardData = [];
            const currentDate = moment().add(5, "hours").add(30, "minutes").subtract(1, "day")
                .toDate();
            // Structure data from array to array of objects
            for (let i = 0; i < studentIds.length; i++) {
                const obj = {
                    student_id: parseInt(studentIds[i]),
                    rank: studentRanks[i],
                    wins: parseInt(studentWins[i]),
                    created_at: currentDate, // The data is of previous day leaderboard - subtracting 1 from current day
                };
                leaderboardData.push(obj);
            }
            job.progress(70);
            let i; let j; const chunk = 1000;
            // Storing data in a batch of 1000
            for (i = 0, j = 50000; i < j; i += chunk) {
                const data = leaderboardData.slice(i, i + chunk);
                if (_.isEmpty(data)) {
                    console.log("Empty leaderboard data");
                    break;
                }
                client.collection(leaderboardCollection).insertMany(data, { ordered: false });
            }
            await delPreviousDayLeaderboardList();
        }
        job.progress(90);
        console.log("task completed");
    } catch (e) {
        console.error(e);
    } finally {
        console.log(`The script successfully ran at ${moment().add(5, "hours").add(30, "minutes")}`);
    }
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "8 0 * * *", // At 12:08 AM everyday
};
