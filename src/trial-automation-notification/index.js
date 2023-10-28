/* eslint-disable no-await-in-loop */
const moment = require("moment");
const { mysql, kafka } = require("../../modules");
const Data = require("./notification.data");

function getActiveTrial(assortmentList) {
    const sql = `select a.*, b.assortment_id, c.gcm_reg_id from (select * from student_package_subscription where start_date < now() and end_date > now() and amount = -1) as a inner join (select * from package) as b on a.new_package_id=b.id inner join (select student_id, gcm_reg_id from students) as c on a.student_id=c.student_id where b.assortment_id in (${assortmentList}) and c.gcm_reg_id is not null order by b.assortment_id asc`;
    return mysql.pool.query(sql).then(([res]) => res);
}

function getAllVideos(assortmentId) {
    const sql = `SELECT cr.*, a.*, crm4.course_resource_id,crm4.name,crm4.resource_type,crm4.live_at, crm3.assortment_id as chapter_assortment from (SELECT DISTINCT assortment_id, display_name,is_free,meta_info as assortment_locale,category_type, start_date as course_session FROM course_details WHERE assortment_id = ${assortmentId} and parent = 1  and category_type in ('BOARDS/SCHOOL/TUITION','BANKING','RAILWAY')) as a left join course_resource_mapping crm1 on a.assortment_id=crm1.assortment_id and crm1.resource_type like "assortment" left join course_resource_mapping crm2 on crm1.course_resource_id=crm2.assortment_id and crm2.resource_type like "assortment" left join course_resource_mapping crm3 on crm2.course_resource_id=crm3.assortment_id and crm3.resource_type like "assortment" left join course_resource_mapping crm4 on crm3.course_resource_id=crm4.assortment_id and crm4.resource_type like "resource" left join course_resources cr on crm4.course_resource_id=cr.id where crm4.live_at is not null  and crm4.is_replay=0 and cr.resource_type in (1,8) group by cr.id order by assortment_locale, crm4.live_at asc`;
    return mysql.pool.query(sql).then(([res]) => res);
}

function checkVideoWatch(questionId, studentId) {
    const sql = `select * from video_view_stats where question_id = ${+questionId} and student_id=${studentId} order by view_id desc limit 1`;
    return mysql.pool.query(sql).then(([res]) => res);
}
function checkVideoWatchOffset(questionId, studentId, offset) {
    const sql = `select * from video_view_stats where question_id = ${+questionId} and student_id=${studentId} and created_at < DATE_SUB(now(),INTERVAL ${offset} HOUR) order by view_id desc limit 1`;
    return mysql.pool.query(sql).then(([res]) => res);
}

function makeUniqueArray(spsDetails, uniqueArr) {
    const obj = uniqueArr.find((o) => o.student_id === spsDetails.student_id);
    if (typeof obj === "undefined") {
        uniqueArr.push(spsDetails);
    }
    return uniqueArr;
}

async function handleTrialActivateNotification(userSet, assortmentVideoHash) {
    for (let i = 0; i < userSet.length; i++) {
        // first video qid
        const notificationPayload = {
            event: "video",
            firebase_eventtag: Data.trialActivate("IBPS", 3).firebaseTag,
            title: Data.trialActivate("IBPS", 3).title,
            message: Data.trialActivate("IBPS", 3).subtitle,
            s_n_id: Data.trialActivate("IBPS", 3).firebaseTag,
            data: {
                qid: assortmentVideoHash[userSet[i].assortment_id].list[0].resource_reference,
                page: "LIVECLASS_NOTIFICATION",
            },
        };
        // console.log(notificationPayload);

        // send notification
        await kafka.sendNotification(
            [userSet[i].student_id], [userSet[i].gcm_reg_id],
            notificationPayload,
        );
        // await kafka.sendNotification(
        //     [2524641], ["fUns27vJRRiSZnXjCRqPSg:APA91bHd2Dl6ZfJhHxaWdB0MF_i8Am4hjJwlih-K4nnEmlOrfyfH2gXwiUTSXHduFWZe1xfkJZ44Py_Gp5hPF69cvTgFeS8wbVTdbxpeCCZLHN9sHykXVzhOCPAvUHz_yBf0Wp572IgX"],
        //     notificationPayload,
        // );
    }
}

async function handleLastTwoDaysNotification(userSet) {
    for (let i = 0; i < userSet.length; i++) {
        // first video qid
        const notificationPayload = {
            event: "course_details",
            firebase_eventtag: Data.trialEndDay2("IBPS").firebaseTag,
            title: Data.trialEndDay2("IBPS").title,
            message: Data.trialEndDay2("IBPS").subtitle,
            data: JSON.stringify({
                id: userSet[i].assortment_id,
            }),
        };
        // console.log(notificationPayload);
        // send notification
        await kafka.sendNotification(
            [userSet[i].student_id], [userSet[i].gcm_reg_id],
            notificationPayload,
        );
        // await kafka.sendNotification(
        //     [2524641], ["fUns27vJRRiSZnXjCRqPSg:APA91bHd2Dl6ZfJhHxaWdB0MF_i8Am4hjJwlih-K4nnEmlOrfyfH2gXwiUTSXHduFWZe1xfkJZ44Py_Gp5hPF69cvTgFeS8wbVTdbxpeCCZLHN9sHykXVzhOCPAvUHz_yBf0Wp572IgX"],
        //     notificationPayload,
        // );
    }
}

async function handleLastOneDaysNotification(userSet) {
    for (let i = 0; i < userSet.length; i++) {
        // first video qid
        const notificationPayload = {
            event: "course_details",
            firebase_eventtag: Data.trialEndDay1("IBPS").firebaseTag,
            title: Data.trialEndDay1("IBPS").title,
            message: Data.trialEndDay1("IBPS").subtitle,
            data: JSON.stringify({
                id: userSet[i].assortment_id,
            }),
        };
        // console.log(notificationPayload);
        // send notification
        await kafka.sendNotification(
            [userSet[i].student_id], [userSet[i].gcm_reg_id],
            notificationPayload,
        );
        // await kafka.sendNotification(
        //     [2524641], ["fUns27vJRRiSZnXjCRqPSg:APA91bHd2Dl6ZfJhHxaWdB0MF_i8Am4hjJwlih-K4nnEmlOrfyfH2gXwiUTSXHduFWZe1xfkJZ44Py_Gp5hPF69cvTgFeS8wbVTdbxpeCCZLHN9sHykXVzhOCPAvUHz_yBf0Wp572IgX"],
        //     notificationPayload,
        // );
    }
}

async function handleMorningNotification(userSet, assortmentVideoHash) {
    // user video watch user hash
    const userHash = {};
    for (let i = 0; i < userSet.length; i++) {
        userHash[userSet[i].student_id] = {};
        userHash[userSet[i].student_id].videoStats = [];
        for (let j = 0; j < assortmentVideoHash[userSet[i].assortment_id].list.length; j++) {
            const vvs = await checkVideoWatch(assortmentVideoHash[userSet[i].assortment_id].list[j].resource_reference, userSet[i].student_id);
            userHash[userSet[i].student_id].videoStats.push({ question_id: assortmentVideoHash[userSet[i].assortment_id].list[j].resource_reference, vvs });
        }
        const filtered = userHash[userSet[i].student_id].videoStats.filter((item) => item.vvs.length > 0);

        if (filtered.length === 0) {
            // send first video notification
            const notificationPayload = {
                event: "video",
                firebase_eventtag: Data.trialMorning("IBPS").firebaseTag,
                title: Data.trialMorning("IBPS").title,
                message: Data.trialMorning("IBPS").subtitle,
                data: JSON.stringify({
                    qid: assortmentVideoHash[userSet[i].assortment_id].list[0].resource_reference,
                    page: "LIVECLASS_NOTIFICATION",
                }),
            };
            // console.log(notificationPayload);
            // send notification
            await kafka.sendNotification(
                [userSet[i].student_id], [userSet[i].gcm_reg_id],
                notificationPayload,
            );
            // await kafka.sendNotification(
            //     [2524641], ["fUns27vJRRiSZnXjCRqPSg:APA91bHd2Dl6ZfJhHxaWdB0MF_i8Am4hjJwlih-K4nnEmlOrfyfH2gXwiUTSXHduFWZe1xfkJZ44Py_Gp5hPF69cvTgFeS8wbVTdbxpeCCZLHN9sHykXVzhOCPAvUHz_yBf0Wp572IgX"],
            //     notificationPayload,
            // );
        } else {
            // send notification which is last watched

            const obj = filtered[filtered.length - 1];
            // console.log(obj);
            const notificationPayload = {
                event: "video",
                firebase_eventtag: Data.trialMorning("IBPS").firebaseTag,
                title: Data.trialMorning("IBPS").title,
                message: Data.trialMorning("IBPS").subtitle,
                data: JSON.stringify({
                    qid: obj.question_id,
                    page: "LIVECLASS_NOTIFICATION",
                    video_start_position: obj.vvs[0].video_time,
                }),
            };
            console.log(notificationPayload);
            // send notification
            await kafka.sendNotification(
                [userSet[i].student_id], [userSet[i].gcm_reg_id],
                notificationPayload,
            );
        }
    }
}
async function handleNightNotification(userSet, assortmentVideoHash) {
    // user video watch user hash
    const userHash = {};
    for (let i = 0; i < userSet.length; i++) {
        userHash[userSet[i].student_id] = {};
        userHash[userSet[i].student_id].videoStats = [];
        for (let j = 0; j < assortmentVideoHash[userSet[i].assortment_id].list.length; j++) {
            const vvs = await checkVideoWatch(assortmentVideoHash[userSet[i].assortment_id].list[j].resource_reference, userSet[i].student_id);
            userHash[userSet[i].student_id].videoStats.push({ question_id: assortmentVideoHash[userSet[i].assortment_id].list[j].resource_reference, vvs });
        }
        const filtered = userHash[userSet[i].student_id].videoStats.filter((item) => item.vvs.length > 0);

        if (filtered.length === 0) {
            // send first video notification
            const notificationPayload = {
                event: "video",
                firebase_eventtag: Data.trialNight("IBPS").firebaseTag,
                title: Data.trialNight("IBPS").title,
                message: Data.trialNight("IBPS").subtitle,
                data: JSON.stringify({
                    qid: assortmentVideoHash[userSet[i].assortment_id].list[0].resource_reference,
                    page: "LIVECLASS_NOTIFICATION",
                }),
            };
            // console.log(notificationPayload);
            // send notification
            await kafka.sendNotification(
                [userSet[i].student_id], [userSet[i].gcm_reg_id],
                notificationPayload,
            );
        } else {
            // send notification which is last watched

            const obj = filtered[filtered.length - 1];
            const notificationPayload = {
                event: "video",
                firebase_eventtag: Data.trialNight("IBPS").firebaseTag,
                title: Data.trialNight("IBPS").title,
                message: Data.trialNight("IBPS").subtitle,
                data: JSON.stringify({
                    qid: obj.question_id,
                    page: "LIVECLASS_NOTIFICATION",
                    video_start_position: obj.vvs[0].video_time,
                }),
            };
            // console.log(notificationPayload);

            // send notification
            await kafka.sendNotification(
                [userSet[i].student_id], [userSet[i].gcm_reg_id],
                notificationPayload,
            );
        }
    }
}
async function handleNoActivityNotification(userSet, assortmentVideoHash, now) {
    // console.log("handle");
    const userHash = {};
    for (let i = 0; i < userSet.length; i++) {
        userHash[userSet[i].student_id] = {};
        userHash[userSet[i].student_id].videoStats = [];
        for (let j = 0; j < assortmentVideoHash[userSet[i].assortment_id].list.length; j++) {
            const vvs = await checkVideoWatch(assortmentVideoHash[userSet[i].assortment_id].list[j].resource_reference, userSet[i].student_id);
            // userHash[userSet[i].student_id].videoStats.push({ question_id: assortmentVideoHash[userSet[i].assortment_id].list[j].resource_reference, vvs });
            if (vvs.length > 0) {
                // check if it is 6 hour gap
                const minuteDiff = now.diff(moment(vvs[0].created_at), "minutes");
                if (minuteDiff > 359 && minuteDiff < 366) {
                    // send notification
                    const notificationPayload = {
                        event: "video",
                        firebase_eventtag: Data.trialReturn6("IBPS").firebaseTag,
                        title: Data.trialReturn6("IBPS").title,
                        message: Data.trialReturn6("IBPS").subtitle,
                        data: JSON.stringify({
                            qid: vvs[0].question_id,
                            page: "LIVECLASS_NOTIFICATION",
                            video_start_position: vvs[0].video_time,
                        }),
                    };
                    await kafka.sendNotification(
                        [userSet[i].student_id], [userSet[i].gcm_reg_id],
                        notificationPayload,
                    );
                }
                if (minuteDiff > 719 && minuteDiff < 726) {
                    const notificationPayload = {
                        event: "video",
                        firebase_eventtag: Data.trialReturn12("IBPS").firebaseTag,
                        title: Data.trialReturn12("IBPS").title,
                        message: Data.trialReturn12("IBPS").subtitle,
                        data: JSON.stringify({
                            qid: vvs[0].question_id,
                            page: "LIVECLASS_NOTIFICATION",
                            video_start_position: vvs[0].video_time,
                        }),
                    };
                    await kafka.sendNotification(
                        [userSet[i].student_id], [userSet[i].gcm_reg_id],
                        notificationPayload,
                    );
                }
            }
        }
    }
}
async function start(job) {
    try {
        const assortmentIdList = [465140, 495269];
        const assortmentVideoHash = {};
        for (let i = 0; i < assortmentIdList.length; i++) {
            if (typeof assortmentVideoHash[assortmentIdList[i]] === "undefined") {
                assortmentVideoHash[assortmentIdList[i]] = {};
            }
            assortmentVideoHash[assortmentIdList[i]].list = await getAllVideos(assortmentIdList[i]);
        }

        const now = moment().add(5, "hours").add(30, "minutes");
        // const now = moment();
        const hourMinFormat = now.format("HH:mm");
        const trials = await getActiveTrial(assortmentIdList);
        // console.log(trials.length);
        let trialActiveUserSet = [];
        let morningNotificationSet = [];
        let nightNotificationSet = [];
        let lastTwoDaysNotificationSet = [];
        let lastOneDaysNotificationSet = [];
        let totalUserSet = [];
        for (let i = 0; i < trials.length; i++) {
            totalUserSet = makeUniqueArray(trials[i], totalUserSet);
            // Handle actived user
            // get last 5 min activated trials
            const minuteDiff = now.diff(moment(trials[i].created_at), "minutes");
            if (minuteDiff <= 5) {
                trialActiveUserSet = makeUniqueArray(trials[i], trialActiveUserSet);
            }
            // Morning notifcation
            // check if it is 10:45 AM
            if (hourMinFormat === "10:45") {
            // if (1) {
                morningNotificationSet = makeUniqueArray(trials[i], morningNotificationSet);
            }
            // Night notifcation
            // check if it is 10:45 in the morning
            if (hourMinFormat === "22:45") {
                nightNotificationSet = makeUniqueArray(trials[i], nightNotificationSet);
            }
            const daysDiff = moment(trials[i].end_date).diff(now, "days");
            // last 2 days trial end

            if (daysDiff === 2 && hourMinFormat === "20:45") {
                lastTwoDaysNotificationSet = makeUniqueArray(trials[i], lastTwoDaysNotificationSet);
            }
            // last 1 days trial end
            if (daysDiff === 1 && hourMinFormat === "11:45") {
                lastOneDaysNotificationSet = makeUniqueArray(trials[i], lastOneDaysNotificationSet);
            }
        }
        await handleTrialActivateNotification(trialActiveUserSet, assortmentVideoHash);
        await handleMorningNotification(morningNotificationSet, assortmentVideoHash);
        await handleNightNotification(morningNotificationSet, assortmentVideoHash);
        await handleLastTwoDaysNotification(lastTwoDaysNotificationSet);
        await handleLastOneDaysNotification(lastOneDaysNotificationSet);
        await handleNoActivityNotification(totalUserSet, assortmentVideoHash, now);
        // console.log("trials");
        // console.log(trials);
        job.progress(10);
    } catch (err) {
        console.log(err);
        return { err };
    } finally {
        console.log(`The script successfully ran at ${new Date()}`);
    }
    job.progress(100);
    return 1;
}

module.exports.start = start;
module.exports.opts = {
    cron: "*/5 * * * *",
};
