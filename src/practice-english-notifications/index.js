const { redis, kafka, config } = require("../../modules");

const BATCH_SIZE = 50;
const yesterDaysDate = () => {
    const dt = new Date();
    dt.setDate(dt.getDate() - 1);
    return dt.toISOString().slice(0, 10);
};

const practiceEnglishTemplate = {
    title: "English bolna hua ab aur bhi asaan! 🗣️",
    message: "Practice karne ke liye aajao 🤩",
    image_url: `${config.staticCDN}images/2021/12/24/05-20-26-182-AM_frame_16.webp`,
    firebaseTag: "PRACTICE_ENGLISH_DAILY_NOTIFICATION",
    event: "practice_english",
    data: {
        deeplink: "doubtnutapp://practice_english",
    },
};

async function sendNotification(allUsers) {
    for (let i = 0; i < allUsers.length; i += BATCH_SIZE) {
        const currUsers = allUsers.slice(i, i + BATCH_SIZE);

        const studentIds = [];
        const gcmIds = [];
        for (let j = 0; j < currUsers.length; j++) {
            studentIds.push(currUsers[j].studentId);
            gcmIds.push(currUsers[j].gcmRegId);
        }
        const notificationPayload = { ...practiceEnglishTemplate };

        kafka.sendNotification(
            studentIds,
            gcmIds,
            notificationPayload,
        );
    }
}

async function start(job) {
    try {
        job.progress(0);
        const redisUsers = await redis.smembers(`PracticeEnglish:REMINDERS:${yesterDaysDate()}`);
        const allUsers = redisUsers.map((eachUser) => JSON.parse(eachUser));

        await sendNotification(allUsers);
    } catch (e) {
        console.error(e);
    }
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 17 * * *", // Daily at 5.00 PM
};
