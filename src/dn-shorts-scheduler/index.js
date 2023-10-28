/* eslint-disable no-await-in-loop */
const moment = require("moment");
const { mysql } = require("../../modules");

const VIDEO_COUNT_REQUIRED_PER_DAY = 150;
const TOTAL_DAYS_FOR_PRE_SCHEDULING = 7;

async function getCategories() {
    const sql = "select distinct(category) as category from dn_shorts_videos";
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getQuestionsByCategory(category, limit) {
    const sql = `select * from dn_shorts_videos where category = '${category}' and show_schedule ='0000-00-00' and is_approved= 1 order by RAND() limit ${limit}`;
    // console.log(`executing=> ${sql}`);
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getRandomQuestions(limit) {
    const sql = `select * from dn_shorts_videos where show_schedule ='0000-00-00' and is_approved= 1 order by RAND() limit ${limit}`;
    // console.log(`executing=> ${sql}`);
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getTotalScheduledVideoDayWise(date) {
    const sql = `select count(1) as total from dn_shorts_videos where show_schedule='${date}' and is_approved=1`;
    console.log(`executing=> ${sql}`);
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function updateScheduleById(obj, id) {
    const sql = "UPDATE dn_shorts_videos SET ? where id = ?";
    return mysql.writePool.query(sql, [obj, id]).then((res) => res[0]);
}

async function updateScheduleData(videosData, updateObj) {
    try {
        const updatePromises = [];
        for (let j = 0; j < videosData.length; j++) {
            updatePromises.push(updateScheduleById(updateObj, videosData[j].id));
        }
        await Promise.all(updatePromises);
    } catch (err) {
        console.log(err);
        return { err };
    }
}

async function scheduleShorts(scheduleDate, categories, videoCountRequiredPerDay) {
    try {
        let categoryCount = categories.length;
        let videoCount = videoCountRequiredPerDay;
        const updateObj = {
            show_schedule: scheduleDate,
        };
        for (let i = 0; i < categories.length; i++) {
            const categorySubCount = Math.round(videoCount / categoryCount);
            const { category } = categories[i];
            console.log(`scheduling shorts for ${category}, total videos => ${categorySubCount}, date => ${scheduleDate}`);
            const videosToSchedule = await getQuestionsByCategory(category, categorySubCount);
            videoCount -= videosToSchedule.length;
            categoryCount -= 1;
            console.log(`total ${videosToSchedule.length} found for ${category}  for the date of ${scheduleDate}`);
            await updateScheduleData(videosToSchedule, updateObj);
            console.log(`scheduled category wise videos for ${category} on ${scheduleDate}`);
        }
        // in case still video count not matched, randomly pick those videos
        if (videoCount >= 0) {
            console.log(`Video Count still ${videoCount}, scheduling random total left videos`);
            const randomVideos = await getRandomQuestions(videoCount);
            console.log(`found ${randomVideos.length} random videos`);
            await updateScheduleData(randomVideos, updateObj);
            console.log("scheduled random videos for ", scheduleDate);
        }
    } catch (err) {
        console.log(err);
        return { err };
    }
}

async function start(job) {
    try {
        const categories = await getCategories();
        console.log(`total ${categories.length} different categories found`);
        job.progress(10);
        for (let i = 0; i <= TOTAL_DAYS_FOR_PRE_SCHEDULING; i++) {
            const istFormattedMomentObj = moment().add(5, "hours").add(30, "minutes");
            const date = istFormattedMomentObj.add(i, "day").format("YYYY-MM-DD");
            console.log(`processing for ${date}, ${i}`);
            const totalScheduled = await getTotalScheduledVideoDayWise(date);
            console.log(`found ${totalScheduled[0].total} videos on ${date}`);
            if (totalScheduled[0].total < VIDEO_COUNT_REQUIRED_PER_DAY) {
                console.log(`total scheduled video is less than ${VIDEO_COUNT_REQUIRED_PER_DAY} for ${date}`);
                console.log("requesting for shorts scheduling");
                await scheduleShorts(date, categories, VIDEO_COUNT_REQUIRED_PER_DAY - totalScheduled[0].total);
                console.log(`videos scheduled for ${date}`);
            }
        }
        job.progress(100);
    } catch (err) {
        console.log(err);
        return { err };
    }
}
module.exports.start = start;
module.exports.opts = {
    // At 0 minutes past the hour, every 12 hours
    cron: "0 */12 * * *",
};
