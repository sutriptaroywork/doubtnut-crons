const moment = require("moment");
const {
    redis, mysql, kafka, config,
} = require("../../modules");

const HASH_EXPIRY = 60 * 60 * 24; // 1day
const prevTimeHours = (hours) => {
    const startTime = moment().add(5, "hours").add(30, "minutes");
    startTime.subtract(hours, "hours");
    return startTime.toISOString();
};

function getNewStudents(currDate, prevTime, statuses) {
    const sql = `SELECT *,  100 - attempts as remaining_attempts from (
        SELECT  pes.student_id, COUNT(*) as attempts, st.gcm_reg_id from practice_english_sessions pes 
            left join students st on pes.student_id = st.student_id 
             where pes.created_at > ? and pes.created_at < ? and pes.status in (?) and st.gcm_reg_id is not null GROUP  by pes.student_id 
        ) af where  af.attempts < 100`;
    return mysql.pool.query(sql, [currDate, prevTime, statuses]).then((res) => res[0]);
}

function getStudentSeenQues(currDate, prevTime, statuses) {
    const sql = `SELECT * from (
        SELECT  pes.student_id,pes.session_id, per.id, st.gcm_reg_id from practice_english_sessions pes 
        	left join practice_english_responses per on pes.session_id = per.session_id 
            left join students st on pes.student_id = st.student_id 
             where pes.created_at > ? and pes.created_at < ? and per.id is null and pes.status in (?) and st.gcm_reg_id is not null GROUP  by pes.student_id 
        ) af `;
    return mysql.pool.query(sql, [currDate, prevTime, statuses]).then((res) => res[0]);
}
async function sendNotification(allUsers, notifType) {
    const allSids = [];
    const allGCMids = [];

    let notificationPayload = {};
    if (notifType === "session_dropoff_notif") {
        notificationPayload = {
            title: "Angrezi bologe nahi, to seekhoge kaise? 🥸",
            message: "5 min me 5 sawaal niptao! English improve karo! 🤩",
            // image: `${config.staticCDN}images/2022/01/28/11-21-47-574-AM_Quiz%20War%20Banner%202_3.webp`,
            firebase_eventtag: "english_session_dropoff",
            event: "english_session_dropoff",
            data: {
                deeplink: "doubtnutapp://practice_english",
            },
        };
    } else if (notifType === "question_dropoff_notif") {
        notificationPayload = {
            title: "Arey sawaal dekh kar darr gaye kya! 🤥",
            message: "English practice se hi seekhi jaati hai dost! Wapas ajao! 😉",
            // image: `${config.staticCDN}images/2022/01/28/11-21-47-574-AM_Quiz%20War%20Banner%202_3.webp`,
            firebase_eventtag: "english_daily_reminder",
            event: "english_daily_reminder",
            data: {
                deeplink: "doubtnutapp://practice_english",
            },
        };
    }
    for (let i = 0; i < allUsers.length; i++) {
        allSids.push(allUsers[i].student_id);
        allGCMids.push(allUsers[i].gcm_reg_id);
    }
    return kafka.sendNotification(
        allSids,
        allGCMids,
        notificationPayload,
    );
}

function isSameStudent(a, b) {
    // eslint-disable-next-line eqeqeq
    return a.student_id == b.student_id;
}

function listDiffObjects(first, second, compareFunction) {
    return first.filter((firObj) => !second.some((secObj) => compareFunction(firObj, secObj)));
}

async function sendDailyNotification() {
    const prevTime = prevTimeHours(3);
    const currTime = prevTimeHours(0);
    const day3AgoTime = prevTimeHours(72);

    const day3AgoDate = day3AgoTime.slice(0, 10);
    const currDate = currTime.slice(0, 10);

    console.log(prevTime, currTime, day3AgoTime);
    const todaysStudents = await getNewStudents(day3AgoDate, currTime, [0, 1]);
    const studentsReAttempted = await getNewStudents(prevTime, currTime, [0, 1]);

    const rem = listDiffObjects(todaysStudents, studentsReAttempted, isSameStudent);

    const sentTodayRedis = await redis.smembers(`PracticeEnglish:CONTEST_REMINDERS:${currDate}`);
    const sentToday = sentTodayRedis.map((eachUser) => JSON.parse(eachUser));

    const newNotifs = listDiffObjects(rem, sentToday, isSameStudent);

    console.log(todaysStudents.length);
    console.log(studentsReAttempted.length);
    console.log(rem.length);
    console.log(sentToday.length);
    console.log(newNotifs.length);

    newNotifs.unshift({
        student_id: 40350141,
        gcm_reg_id: "dT5T0J8XTCmF3vmE5n8XrQ:APA91bE-H5SX0oHRGc9jCNt2rFbkvxrCU0yjbKmeUOtnkZR_5elLkNj8m9v6-WvOnWd4fT-Pnuapbp8LoMknf0xRV6VXbC2SCE7LfnPzHCdmrc2U-Z4xRR9dcKkOlPWxEb3r9_QCboaI",
        remaining_attempts: 31,
    });

    await sendNotification(newNotifs, "hourly_notif");

    if (newNotifs.length > 0) {
        for (let i = 0; i < newNotifs.length; i += 500) {
            const batchNotif = newNotifs.slice(i, i + 500);
            // eslint-disable-next-line no-await-in-loop
            await redis.multi()
                .sadd(`PracticeEnglish:CONTEST_REMINDERS:${currDate}`, ...batchNotif.map((obj) => JSON.stringify({ student_id: obj.student_id })))
                .expire(`PracticeEnglish:CONTEST_REMINDERS:${currDate}`, HASH_EXPIRY * 7)
                .execAsync();
        }
    }
}

async function sendSessionDropoffNotification() {
    const prevTime = prevTimeHours(1);
    const currTime = prevTimeHours(0);
    const prev6HoursTime = prevTimeHours(6);

    const currDateTime = currTime.slice(0, 13);

    const getLandingStudents = await getNewStudents(prev6HoursTime, currTime, [-1]);
    const studentsStarted = await getNewStudents(prevTime, currTime, [0, 1]);

    const rem = listDiffObjects(getLandingStudents, studentsStarted, isSameStudent);

    const sentLast6Hours = [];
    for (let i = 0; i < 6; i++) {
        const tempTime = prevTimeHours(i + 1);
        const eachHourDate = tempTime.slice(0, 13);
        // eslint-disable-next-line no-await-in-loop
        const sentStudentRedis = await redis.smembers(`PracticeEnglish:SESSION_DROPOFF:${eachHourDate}`);
        console.log("here", eachHourDate, sentStudentRedis.length);
        const LastSent = sentStudentRedis.map((eachUser) => JSON.parse(eachUser));
        sentLast6Hours.push(...LastSent);
    }
    const newNotifs = listDiffObjects(rem, sentLast6Hours, isSameStudent);

    console.log(getLandingStudents.length);
    console.log(studentsStarted.length);
    console.log(rem.length);
    console.log(sentLast6Hours.length);
    console.log(newNotifs.length);

    // console.log(newNotifs[0]);
    await sendNotification(newNotifs, "session_dropoff_notif");

    if (newNotifs.length > 0) {
        for (let i = 0; i < newNotifs.length; i += 500) {
            const batchNotif = newNotifs.slice(i, i + 500);
            // eslint-disable-next-line no-await-in-loop
            await redis.multi()
                .sadd(`PracticeEnglish:SESSION_DROPOFF:${currDateTime}`, ...batchNotif.map((obj) => JSON.stringify({ student_id: obj.student_id })))
                .expire(`PracticeEnglish:SESSION_DROPOFF:${currDateTime}`, HASH_EXPIRY)
                .execAsync();
        }
    }
}

async function sendQuestionDropoffNotification() {
    const prevTime = prevTimeHours(1);
    const currTime = prevTimeHours(0);
    const prev6HoursTime = prevTimeHours(6);

    const currDateTime = currTime.slice(0, 13);

    const getStartingStudents = await getNewStudents(prev6HoursTime, currTime, [0]);
    const studentsAttempted = await getStudentSeenQues(prevTime, currTime, [0, 1]);

    const rem = listDiffObjects(getStartingStudents, studentsAttempted, isSameStudent);

    const sentLast6Hours = [];
    for (let i = 0; i < 6; i++) {
        const tempTime = prevTimeHours(i + 1);
        const eachHourDate = tempTime.slice(0, 13);
        // eslint-disable-next-line no-await-in-loop
        const sentStudentRedis = await redis.smembers(`PracticeEnglish:QUESTION_DROPOFF:${eachHourDate}`);
        console.log("here", eachHourDate, sentStudentRedis.length);
        const LastSent = sentStudentRedis.map((eachUser) => JSON.parse(eachUser));
        sentLast6Hours.push(...LastSent);
    }

    const newNotifs = listDiffObjects(rem, sentLast6Hours, isSameStudent);

    console.log(getStartingStudents.length);
    console.log(studentsAttempted.length);
    console.log(rem.length);
    console.log(sentLast6Hours.length);
    console.log(newNotifs.length);

    await sendNotification(newNotifs, "question_dropoff_notif");

    if (newNotifs.length > 0) {
        for (let i = 0; i < newNotifs.length; i += 500) {
            const batchNotif = newNotifs.slice(i, i + 500);
            // eslint-disable-next-line no-await-in-loop
            await redis.multi()
                .sadd(`PracticeEnglish:QUESTION_DROPOFF:${currDateTime}`, ...batchNotif.map((obj) => JSON.stringify({ student_id: obj.student_id })))
                .expire(`PracticeEnglish:QUESTION_DROPOFF:${currDateTime}`, HASH_EXPIRY)
                .execAsync();
        }
    }
}
async function start(job) {
    try {
        job.progress(0);

        await sendSessionDropoffNotification();
        await sendQuestionDropoffNotification();
    } catch (e) {
        console.error(e);
    }
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 7-23 * * *", // Daily at 5.00 PM
};
