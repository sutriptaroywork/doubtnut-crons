/* eslint-disable no-await-in-loop */
const moment = require("moment");
const { getMongoClient, kafka, mysql } = require("../../modules");
const redisClient = require("../../modules/redis");

/** This cron sends notification to the users who have marked attendance yesterday */

const db = "doubtnut";
const rewardCollection = "rewards";

async function getByKey(key, client) {
    return client.getAsync(key);
}

async function getAllRewardDays(versionCode, lastMarkedAttendance, client) {
    try {
        let rewardsDays = await getByKey(`ALL_REWARD_DAYS_${versionCode}`, redisClient);
        if (!rewardsDays || !rewardsDays.length) {
            console.error("redis key not found for ALL_REWARD_DAYS");
            rewardsDays = [];
            const allRewards = await client.collection(rewardCollection).find({
                min_app_version: { $lte: versionCode }, max_app_version: { $gte: versionCode },
            }).sort({ level: 1 }).toArray();
            for (let j = 0; j < allRewards.length; j++) {
                rewardsDays.push(allRewards[j].day);
            }
        }
        if (typeof rewardsDays === "string") {
            rewardsDays = JSON.parse(rewardsDays);
        }
        let i = 0; let nextGreaterRewardDay = 0;
        while (i < rewardsDays.length) {
            if (rewardsDays[i] > lastMarkedAttendance) {
                nextGreaterRewardDay = rewardsDays[i];
                break;
            }
            i++;
        }
        return nextGreaterRewardDay;
    } catch (e) {
        return 0;
    }
}

async function sendReminder(studentDetails, rewardList, client) {
    const notificationData = {
        event: "rewards",
        message: "Jaldi lagayen attendance and na miss karen streak!",
        image: null,
        firebase_eventtag: "reward_attendance_reminder",
        data: {},
    };

    for (const student of studentDetails) {
        if (student && student.student_id && student.gcm_reg_id) {
            const nextGreaterRewardDay = await getAllRewardDays(student.is_online, student.last_marked_attendance, client);
            const reward = rewardList.filter((item) => item.day === nextGreaterRewardDay);

            if (reward && reward[0]) {
                const rewardData = reward[0].locked_desc.split("•");
                const rewardDesc = rewardData[Math.floor(Math.random() * (rewardData.length - 2) + 1)].replace("\n", "").trim();
                notificationData.title = `${rewardDesc} hai next possible reward!`;
                await kafka.sendNotification([student.student_id], [student.gcm_reg_id], notificationData);
            }
        }
    }
}

function getApplicableStudents(skip, limit, startTime, endTime, client) {
    return client.collection("student_rewards")
        .find({
            is_notification_opted: true,
            last_attendance_timestamp: {
                $gt: startTime,
                $lt: endTime,
            },
        })
        .skip(skip)
        .limit(limit)
        .toArray();
}

function getRewardData(client) {
    return client.collection(rewardCollection)
        .find()
        .toArray();
}

function getStudentDetails(studentList) {
    const sql = `SELECT student_id, gcm_reg_id, locale, is_online FROM students WHERE student_id IN (${studentList})`;
    return mysql.pool.query(sql)
        .then((res) => res[0]);
}

function isAttendanceMarkedYesterday(lastAttendanceTimestamp, startTime, endTime) {
    return moment(lastAttendanceTimestamp).isBetween(moment(startTime), moment(endTime));
}

async function attendanceReminder(client, job) {
    const rewardList = await getRewardData(client);
    job.progress(20);
    const lastDay = moment().subtract(1, "day").format("YYYY-MM-DD");
    const startTime = moment(`${lastDay}T00:00:00Z`).toDate();
    const endTime = moment(`${lastDay}T23:59:59Z`).toDate();
    const chunk = 10000;
    let skip = 0;
    for (let i = 0, j = 5000000; i < j; i += chunk) {
        const applicableStudents = await getApplicableStudents(skip, chunk, startTime, endTime, client);
        if (!applicableStudents.length) {
            console.log("No more data left");
            break;
        }
        const studentIds = [];
        for (let k = 0; k < applicableStudents.length; k++) {
            if (applicableStudents[k] && applicableStudents[k].studentId && isAttendanceMarkedYesterday(applicableStudents[k].last_attendance_timestamp, startTime, endTime)) {
                studentIds.push(applicableStudents[k].studentId);
            }
        }
        const reminderStudentDetails = await getStudentDetails(studentIds);
        const mergedList = reminderStudentDetails.map((t1) => ({ ...t1, ...applicableStudents.find((t2) => t2.studentId === t1.student_id) }));
        await sendReminder(mergedList, rewardList, client);
        skip += chunk;
    }
    job.progress(90);
}

async function start(job) {
    job.progress(10);
    console.log("task started");
    const client = (await getMongoClient()).db(db);
    await attendanceReminder(client, job);
    job.progress(100);
    console.log("task completed");
    return { data: "success" };
}

module.exports.start = start;
module.exports.opts = {
    cron: "5 7 * * *", // At 07:05 AM everyday
};
