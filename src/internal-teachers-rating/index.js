const mysql = require("../../modules/mysql");
const redis = require("../../modules/redis");

function getPackageData() {
    const sql = "SELECT course_resources.faculty_id, ROUND( AVG(liveclass_feedback_response.star_rating) , 3) AS avg_star_rating FROM liveclass_feedback_response , course_resources , dashboard_users WHERE liveclass_feedback_response.detail_id = course_resources.id AND course_resources.faculty_id = dashboard_users.id GROUP BY course_resources.faculty_id";
    return mysql.pool.query(sql).then((res) => res[0]);
}

// eslint-disable-next-line no-unused-vars
async function getTeachersRankingRedis(job) {
    const packageData = await getPackageData();
    // console.log(packageData);

    for (let i = 0; i < packageData.length; i++) {
        if (packageData[i].faculty_id && packageData[i].avg_star_rating) {
            // console.log(packageData[i].avg_star_rating, packageData[i].faculty_id);
            // eslint-disable-next-line no-await-in-loop
            await redis.zadd("teacher_rating", packageData[i].avg_star_rating, packageData[i].faculty_id);
        }
    }

    await redis.zrevrange("teacher_rating", 0, -1, "withscores", (err, members) => {
        console.log(members);
    });
}

async function start(job) {
    const list = await getTeachersRankingRedis(job);
    return { list };
}

module.exports.start = start;

module.exports.opts = {
    cron: "0 0 * * *", // Runs at 12 AM everyday
};
