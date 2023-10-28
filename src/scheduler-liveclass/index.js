/* eslint-disable guard-for-in */
/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const moment = require("moment");

const {
    mysql, redshift, redis,
} = require("../../modules");

// bluebird.promisifyAll(Redis);

function getScheduleDetail() {
    const mysqlQ = "select * from liveclass_scheduler";
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}

function getPlaylistCombinations() {
    const mysqlQ = "select b.*, c.resource_path from (select DISTINCT playlist_id from library_playlists_ccm_mapping where is_active=1 and flag_id = 1) as a left join (select playlist_id, parent_playlist_id,rank_order from library_playlists_faculty_mapping) as b on a.playlist_id=b.playlist_id left join (select id,resource_path from new_library) as c on b.parent_playlist_id=c.id order by b.rank_order asc";
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}
async function getET(qidList) {
    try {
        console.log("inside et details");
        const sql = `select question_id , sum(engage_time) as et from (select view_id , question_id, engage_time from classzoo1.video_view_stats where source like 'android' and question_id  in (${qidList}) group by 1,2,3) group by 1 order by et desc`;
        const users = await redshift.query(sql).then((res) => res);
        return users;
    } catch (e) {
        console.log(e);
    }
}

function checkQids(questionIdArr) {
    const sql = `select question_id from liveclass_scheduler_logs where question_id in (${questionIdArr}) and is_active = 1`;
    return mysql.pool.query(sql).then(([res]) => res);
}
function truncateSchedulerLogs() {
    const sql = "DELETE FROM liveclass_scheduler_logs WHERE created_at < DATE_SUB(now(), INTERVAL 30 DAY) and is_active=1";
    return mysql.writePool.query(sql).then(([res]) => res);
}
function addSchedulerLog(data) {
    // console.log(data);
    const sql = "insert into liveclass_scheduler_logs set ?";
    return mysql.writePool.query(sql, data).then(([res]) => res);
}

function redisSet(date, slot, day, playlistId, questionId) {
    const key = `LIVECLASS_SCHEDULER::${date}:${slot}:${day}:${playlistId}`;
    return redis.multi()
        .sadd(key, questionId)
        .expire(key, 60 * 60 * 24)
        .execAsync();
}
function getRedisSet(date, slot, day, playlistId) {
    const key = `LIVECLASS_SCHEDULER::${date}:${slot}:${day}:${playlistId}`;
    console.log(key);
    // console.log(key);
    return redis.smembersAsync(key);
}
async function start(job) {
    try {
        // eslint-disable-next-line no-unused-vars
        const [schedule, playlistDetails, truncateLogs] = await Promise.all([getScheduleDetail(), getPlaylistCombinations(), truncateSchedulerLogs()]);
        // const forwardHour = 2;
        const forwardDay = 1;
        const monthOld = 1;
        const threeMonthOld = moment().add(forwardDay, "days").add(5, "h").add(30, "minutes")
            .subtract(monthOld, "months");
        const currentDay = moment().add(forwardDay, "days").add(5, "h").add(30, "minutes")
            .format("ddd");
        const hourToCalculate = moment().add(forwardDay, "days").add(5, "h").add(30, "minutes")
            .hour();
        // const hourToCalculate = 17;
        const currentDate = moment().add(forwardDay, "days").add(5, "h").add(30, "minutes")
            .format("DD-MM-YYYY");
        console.log(`Setting data for date - ${currentDate}, hour - ${hourToCalculate}, day - ${currentDay}`);

        const groupedPlaylist = _.groupBy(playlistDetails, "playlist_id");
        // console.log(groupedPlaylist);
        for (let i = 0; i < schedule.length; i++) {
            // console.log("Scheduler");
            // console.log(schedule[i]);
            console.log(hourToCalculate);
            if (schedule[i].days.includes(currentDay) && schedule[i].total_videos > 0 && schedule[i].slots == hourToCalculate) {
                // console.log("add videos");
                // iterate in playlist id
                for (const playlistId in groupedPlaylist) {
                    const videoToPush = [];
                    // console.log("playlistId");
                    // console.log(playlistId);
                    const redisSetMembers = await getRedisSet(currentDate, schedule[i].slots, currentDay, playlistId);
                    if (redisSetMembers.length < schedule[i].total_videos) {
                        for (let j = 0; j < groupedPlaylist[playlistId].length; j++) {
                            if (!_.isNull(groupedPlaylist[playlistId][j].resource_path) && !_.isEmpty(groupedPlaylist[playlistId][j].resource_path)) {
                                const query = groupedPlaylist[playlistId][j].resource_path.replace("Limit 10", "").replace("limit 10", "");
                                // console.log("query");
                                // console.log(query);
                                // get question id list
                                let qidList = await mysql.pool.query(query).then(([res]) => res);
                                qidList = qidList.filter((item) => moment(item.live_at).isBefore(threeMonthOld) && (moment(item.live_at).format("YYYY") >= 2021));
                                // console.log("Question id list");
                                // console.log(qidList);
                                if (qidList.length > 0) {
                                    const groupedQidList = _.groupBy(qidList, "question_id");
                                    let qidArr = qidList.map((item) => item.question_id);
                                    if (qidArr.length > 0) {
                                    // console.log("qidArr");
                                    // console.log(qidArr);
                                        qidArr = qidArr.filter(Boolean);
                                        const usedQidList = await checkQids(qidArr);
                                        const groupedUserQidList = _.groupBy(usedQidList, "question_id");
                                        console.log("before");
                                        console.log(qidArr.length);
                                        // eslint-disable-next-line array-callback-return
                                        qidArr = qidArr.filter((item) => {
                                            if (typeof groupedUserQidList[item] === "undefined") {
                                                return item;
                                            }
                                        });
                                        console.log("after");
                                        console.log(qidArr.length);
                                        if (qidArr.length > 0) {
                                            // sort by engage time
                                            let etDetails = await getET(qidArr);
                                            console.log("got et details");
                                            etDetails = etDetails.map((item) => {
                                                item.subject = groupedQidList[item.question_id][0].subject;
                                                item.faculty_id = groupedQidList[item.question_id][0].faculty_id;
                                                return item;
                                            });
                                            const groupedEtDetailsBySubject = _.groupBy(etDetails, "subject");
                                            console.log("qid sort based on et");
                                            // console.log(etDetails);
                                            const distinctSubject = [...new Set(etDetails.map((item) => item.subject))];
                                            console.log("distinctSubject");
                                            console.log(distinctSubject);
                                            // group by subject in already pushed video array
                                            for (let k = 0; k < distinctSubject.length; k++) {
                                                let counter = 0;
                                                let groupedVideoToPushBySubject = _.groupBy(videoToPush, "subject");
                                                let groupedVideoToPushByFaculty = _.groupBy(videoToPush, "faculty_id");
                                                // console.log("groupedVideoToPushBySubject");
                                                // console.log(groupedVideoToPushBySubject);
                                                // if (typeof groupedVideoToPushBySubject[distinctSubject[k]] === "undefined") {
                                                // qid push logic
                                                while (
                                                    typeof groupedEtDetailsBySubject[distinctSubject[k]][counter] !== "undefined"
                                                && videoToPush.length < schedule[i].total_videos
                                                && (
                                                    (typeof groupedVideoToPushBySubject[distinctSubject[k]] === "undefined")
                                                    || (typeof groupedVideoToPushBySubject[distinctSubject[k]] !== "undefined"
                                                    && groupedVideoToPushBySubject[distinctSubject[k]].length < schedule[i].video_per_subject
                                                    && groupedVideoToPushByFaculty[groupedEtDetailsBySubject[distinctSubject[k]][counter].faculty_id] === "undefined"))) {
                                                // console.log("push");
                                                // console.log(groupedEtDetailsBySubject);
                                                // console.log(distinctSubject[k]);
                                                // console.log(counter);
                                                // console.log(videoToPush);
                                                // const isExist = await checkQid(groupedEtDetailsBySubject[distinctSubject[k]][counter].question_id);
                                                // if (isExist.length === 0) {
                                                // set in mysql and redis
                                                    const createdAt = moment().add(forwardDay, "days").add(5, "h").add(30, "minutes")
                                                        .format("YYYY-MM-DD HH:00:00");
                                                    addSchedulerLog({ question_id: groupedEtDetailsBySubject[distinctSubject[k]][counter].question_id, playlist_id: playlistId, created_at: createdAt });
                                                    redisSet(currentDate, schedule[i].slots, currentDay, playlistId, groupedEtDetailsBySubject[distinctSubject[k]][counter].question_id);
                                                    videoToPush.push(groupedEtDetailsBySubject[distinctSubject[k]][counter]);
                                                    groupedVideoToPushBySubject = _.groupBy(videoToPush, "subject");
                                                    groupedVideoToPushByFaculty = _.groupBy(videoToPush, "faculty_id");
                                                    // }
                                                    counter += 1;
                                                }
                                                // }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    // iterate faculty playlist inside master playlist

                    console.log("videoToPush");
                    console.log(videoToPush);
                }
            }
        }
        job.progress(100);
        console.log(`the script successfully ran at ${new Date()}`);
        return {
            data: {
                done: true,
            },
        };
    } catch (err) {
        console.log(err);
        return { err };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 */1 * * *",
};
