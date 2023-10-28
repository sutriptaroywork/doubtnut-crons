const moment = require("moment");
const _ = require("lodash");
const redisClient = require("../../modules/redis");
const { getMongoClient } = require("../../modules");

/** This cron will set weekly leaderboard from redis to mongo */

const db = "doubtnut";
const leaderboardCollection = "daily_goal_weekly_leaderboard";

async function getLeaderboardList() {
    // To fetch previous week's leaderboard list - we are doing week() - 1
    return redisClient.zrevrangeAsync(`DG_WEEKLY_LEADERBOARD_${moment().add(5, "hours").add(30, "minutes").week() - 1}`, 0, -1, "WITHSCORES");
}

async function delPreviousWeekLeaderboardList() {
    // deleting old redis data after storing it in mongo
    return redisClient.delAsync(`DG_WEEKLY_LEADERBOARD_${moment().add(5, "hours").add(30, "minutes").week() - 1}`);
}

async function start(job) {
    job.progress(10);
    const client = (await getMongoClient()).db(db);
    console.log("task started");
    const leaderboardList = await getLeaderboardList();
    job.progress(30);
    const studentIds = [];
    const studentWins = [];
    const studentRanks = [];
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
        const leaderboardData = [];
        const currentDate = moment().add(5, "hours").add(30, "minutes").toDate();
        const week = moment().add(5, "hours").add(30, "minutes").week() - 1;
        // Structure data from array to array of objects
        for (let i = 0; i < studentIds.length; i++) {
            const obj = {
                student_id: parseInt(studentIds[i]),
                rank: studentRanks[i],
                wins: parseInt(studentWins[i]),
                week, // The data is of previous week leaderboard - subtracting 1 from current week
                created_at: currentDate,
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
        delPreviousWeekLeaderboardList();
    }
    console.log("task completed");
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "11 2 * * 1", // At 02:11 AM on every Monday
};
