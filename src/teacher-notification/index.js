/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const moment = require("moment");
const {
    mysql, kafka, redis, redshift,
} = require("../../modules");

async function getVerifiedTeachers() {
    const sql = "select * from teachers where is_verified = 1 and is_active = 1";
    return mysql.pool.query(sql).then((res) => res[0]);
}

function getNewResourcesByTeacherId(teacherId) {
    const sql = "select cr.* from course_resources cr inner join teachers_resource_upload tru on tru.course_resource_id = cr.id inner join answers a on a.question_id = cr.resource_reference inner join answer_video_resources on avr.answer_id = a.answer_id where tru.is_uploaded = 1 and cr.resource_type = 1 and cr.created_at > DATE_SUB(NOW(), INTERVAL 1 DAY) and avr.resource_type = 'DASH' and cr.vendor_id = 3 and cr.faculty_id = ?";
    return mysql.pool.query(sql, [teacherId]).then((res) => res[0]);
}

async function getSubscriberListByTeacherId(teacherId) {
    const sql = "select s.* from teachers_student_subscription tss inner join students s on s.student_id = tss.student_id where tss.teacher_id = ? and tss.is_active = 1";
    return mysql.pool.query(sql, [teacherId]).then((res) => res[0]);
}

async function getAllVerifiedTeachersResources(teacherId) {
    const sql = "select cr.* from course_resources cr inner join teachers_resource_upload tru on tru.course_resource_id = cr.id inner join answers a on a.question_id = cr.resource_reference inner join answer_video_resources avr on avr.answer_id = a.answer_id where tru.teacher_id in (?) and cr.resource_type = 1 group by cr.id";
    return mysql.pool.query(sql, [teacherId]).then((res) => res[0]);
}

async function getAllStudentsWatchedVideos(resourceIds) {
    const sql = "select s.student_id,s.gcm_reg_id,s.student_class,vvs.question_id,vvs.engage_time from video_view_stats vvs inner join students s on s.student_id = vvs.student_id where vvs.question_id in (?) and vvs.created_at > DATE_SUB(NOW(), INTERVAL 1 DAY)";
    return mysql.pool.query(sql, [resourceIds]).then((res) => res[0]);
}

async function getSubscribedTeacherListByStudentId(studentList) {
    const sql = "select * from teachers_student_subscription where student_id in (?) and is_active = 1";
    return mysql.pool.query(sql, [studentList]).then((res) => res[0]);
}

async function getHighETUsers() {
    const sql = "select student_id, sum(engage_time) as et from video_view_stats where created_at > DATE_SUB(NOW(), INTERVAL 1 DAY) group by student_id having sum(engage_time) > 1000";
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getGcmIdByStudentId(studentList) {
    const sql = `select s.student_id, s.gcm_reg_id from classzoo1.students s where s.student_id in (${studentList}) and s.gcm_reg_id is not null`;
    const users = await redshift.query(sql).then((res) => res);
    return users;
    // return mysql.pool.query(sql, [studentList]).then((res) => res[0]);
}

async function getResourceByTeacherId(teahcerId) {
    const sql = "select cr.* from course_resources cr inner join teachers_resource_upload tru on tru.course_resource_id = cr.id inner join answers a on a.question_id = cr.resource_reference inner join answer_video_resources avr on avr.answer_id = a.answer_id where tru.teacher_id in (?) and cr.resource_type = 1 group by cr.id";
    return mysql.pool.query(sql, [teahcerId]).then((res) => res[0]);
}

async function start(job) {
    try {
        const verifiedTeachers = await getVerifiedTeachers();
        const teacherList = [];
        for (let i = 0; i < verifiedTeachers.length; i += 1) {
            teacherList.push(verifiedTeachers[i].teacher_id);
        }
        if (!_.isEmpty(teacherList)) {
            const time1 = moment().add(5, "hours").add(30, "minutes").startOf("day")
                .add(19, "hours")
                .subtract(5, "minutes")
                .format();
            const time2 = moment().add(5, "hours").add(30, "minutes").startOf("day")
                .add(19, "hours")
                .add(5, "minutes")
                .format();
            const time3 = moment().add(5, "hours").add(30, "minutes").startOf("day")
                .add(20, "hours")
                .subtract(5, "minutes")
                .format();
            const time4 = moment().add(5, "hours").add(30, "minutes").startOf("day")
                .add(20, "hours")
                .add(5, "minutes")
                .format();
            const time5 = moment().add(5, "hours").add(30, "minutes").startOf("day")
                .add(21, "hours")
                .subtract(5, "minutes")
                .format();
            const time6 = moment().add(5, "hours").add(30, "minutes").startOf("day")
                .add(21, "hours")
                .add(5, "minutes")
                .format();

            // 7 PM notification
            if (moment().add(5, "hours").add(30, "minutes").isAfter(time1) && moment().add(5, "hours").add(30, "minutes").isBefore(time2)) {
                for (let i = 0; i < teacherList.length; i++) {
                    const getNewResources = await getNewResourcesByTeacherId(teacherList[i]);
                    if (!_.isEmpty(getNewResources)) {
                        const getSubscriberList = await getSubscriberListByTeacherId(teacherList[i]);
                        if (!_.isEmpty(getSubscriberList)) {
                            const notificationPayload = {
                                event: "video",
                                // image: verifiedTeachers[i].img_url,
                                title: `${verifiedTeachers[i].fname} ${verifiedTeachers[i].lname} ne upload kiya naya video!`,
                                message: "Click karein apne favourite teacher ka naya video dekhne ke liye!",
                                data: JSON.stringify({
                                    qid: getNewResources[0].resource_reference,
                                    page: "HOME_PAGE",
                                    resource_type: "video",
                                    playlist_id: "TEACHER_CHANNEL",
                                }),
                            };
                            const studentIdsList = [];
                            const gcmIdList = [];
                            for (let j = 0; j < getSubscriberList.length; j++) {
                                studentIdsList.push(getSubscriberList[j].student_id);
                                gcmIdList.push(getSubscriberList[j].gcm_id);
                            }
                            await kafka.sendNotification(
                                studentIdsList, gcmIdList, notificationPayload,
                            );
                        }
                    }
                }
            }
            // 8 PM notification
            if (moment().add(5, "hours").add(30, "minutes").isAfter(time3) && moment().add(5, "hours").add(30, "minutes").isBefore(time4)) {
                const allVerifiedTeachersResources = await getAllVerifiedTeachersResources(teacherList);
                const allResourceList = [];
                for (let i = 0; i < allVerifiedTeachersResources.length; i++) {
                    allResourceList.push(allVerifiedTeachersResources[i].resource_reference);
                }
                const allStudentsWatchedVideos = await getAllStudentsWatchedVideos(allResourceList);
                let allStudentsWatchedVideosList = [];
                for (let i = 0; i < allStudentsWatchedVideos.length; i++) {
                    const indexQuestion = allResourceList.indexOf(allStudentsWatchedVideos[i].question_id.toString());
                    const teacherId = allVerifiedTeachersResources[indexQuestion].faculty_id;
                    allStudentsWatchedVideos[i].teacher_id = teacherId;
                    allStudentsWatchedVideosList.push(allStudentsWatchedVideos[i].student_id);
                }
                allStudentsWatchedVideosList = _.uniq(allStudentsWatchedVideosList);
                const studentSubscription = await getSubscribedTeacherListByStudentId(allStudentsWatchedVideosList);
                const allStudentsWatchedVideosListFinal = [];
                for (let i = 0; i < allStudentsWatchedVideos.length; i++) {
                    const check = studentSubscription.filter((item) => item.student_id === allStudentsWatchedVideos[i].student_id && item.teacher_id === allStudentsWatchedVideos[i].teacher_id);
                    if (_.isEmpty(check)) {
                        allStudentsWatchedVideosListFinal.push(allStudentsWatchedVideos[i]);
                    }
                }
                const groupByStudent = _.groupBy(allStudentsWatchedVideosListFinal, "student_id");
                let workers = [];
                for (const key in groupByStudent) {
                    if (groupByStudent[key]) {
                        const highestETTeacher = _.maxBy(groupByStudent[key], "engage_time");
                        const otherWatchedVideos = groupByStudent[key].filter((item) => item.teacher_id === highestETTeacher.teacher_id);
                        const otherWatchedVideosList = [];
                        for (let i = 0; i < otherWatchedVideos.length; i++) {
                            otherWatchedVideosList.push(otherWatchedVideos[i].question_id);
                        }
                        let otherVideosOfTeacher = allVerifiedTeachersResources.filter((item) => item.faculty_id === highestETTeacher.teacher_id);
                        otherVideosOfTeacher = otherVideosOfTeacher.filter((item) => !otherWatchedVideosList.includes(parseInt(item.resource_reference)));
                        if (parseInt(groupByStudent[key][0].student_class) === 13) {
                            groupByStudent[key][0].student_class = 12;
                        }
                        otherVideosOfTeacher = otherVideosOfTeacher.filter((item) => item.class === parseInt(groupByStudent[key][0].student_class));
                        if (!_.isEmpty(otherVideosOfTeacher)) {
                            const index = verifiedTeachers.map((e) => e.teacher_id).indexOf(highestETTeacher.teacher_id);
                            if (index !== -1) {
                                const notificationPayload = {
                                    event: "video",
                                    // image: verifiedTeachers[index].img_url,
                                    title: `${verifiedTeachers[index].fname} ${verifiedTeachers[index].lname} ke dusre videos bhi dekhein!`,
                                    message: "Click karein apne favourite teacher ke aur bhi videos dekhne ke liye!",
                                    data: JSON.stringify({
                                        qid: otherVideosOfTeacher[0].resource_reference,
                                        page: "HOME_PAGE",
                                        resource_type: "video",
                                        playlist_id: "TEACHER_CHANNEL",
                                    }),
                                };
                                const studentIdsList = [key];
                                const gcmIdList = [groupByStudent[key][0].gcm_reg_id];
                                if (workers.length < 99) {
                                    workers.push(kafka.sendNotification(
                                        studentIdsList, gcmIdList, notificationPayload,
                                    ));
                                } else {
                                    await Promise.all(workers);
                                    workers = [];
                                    workers.push(kafka.sendNotification(
                                        studentIdsList, gcmIdList, notificationPayload,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
            // 9 PM notification
            if (moment().add(5, "hours").add(30, "minutes").isAfter(time5) && moment().add(5, "hours").add(30, "minutes").isBefore(time6)) {
                const etGreaterThan1000Users = await getHighETUsers();
                const studentList = [];
                for (let i = 0; i < etGreaterThan1000Users.length; i++) {
                    studentList.push(parseInt(etGreaterThan1000Users[i].student_id));
                }
                let studentWithGcm = await getGcmIdByStudentId(studentList);
                studentWithGcm = studentWithGcm.filter((item) => item.gcm_reg_id !== null);
                studentWithGcm = studentWithGcm.filter((item) => item.gcm_reg_id !== "");
                const studentIdsList = [];
                const gcmIdList = [];
                for (let i = 0; i < studentWithGcm.length; i++) {
                    studentIdsList.push(studentWithGcm[i].student_id);
                    gcmIdList.push(studentWithGcm[i].gcm_reg_id);
                }
                const teachersArr = [];
                const allRanks = await redis.zrevrangeAsync("leaderboard:teachers:weekly", 0, 4, "WITHSCORES");
                if (!_.isEmpty(allRanks)) {
                    for (let j = 0; j < allRanks.length; j++) {
                        if (j % 2 === 0) {
                            teachersArr.push(allRanks[j]);
                        }
                    }
                }
                const tempTeachersArr = [...teachersArr];
                const resourceByTeacherId = await getResourceByTeacherId(tempTeachersArr);
                const groupResourceByTeacherId = _.groupBy(resourceByTeacherId, "faculty_id");
                let part = 0;
                for (const key in groupResourceByTeacherId) {
                    if (groupResourceByTeacherId[key]) {
                        const randomResource = groupResourceByTeacherId[key][Math.floor(Math.random() * groupResourceByTeacherId[key].length)];
                        const index = verifiedTeachers.map((e) => e.teacher_id).indexOf(parseInt(key));
                        if (index !== -1) {
                            const notificationPayload = {
                                event: "video",
                                // image: verifiedTeachers[index2].img_url,
                                title: `${verifiedTeachers[index].fname} ${verifiedTeachers[index].lname} bohot lokpriya teacher hai Doubtnut pe!`,
                                message: "Click karein inke videos abhi dekhne ke liye",
                                data: JSON.stringify({
                                    qid: randomResource.resource_reference,
                                    page: "HOME_PAGE",
                                    resource_type: "video",
                                    playlist_id: "TEACHER_CHANNEL",
                                }),
                            };
                            const studentListTemp = [];
                            const gcmIdListTemp = [];
                            // divide studentIdsList into 5 euqal parts
                            for (let i = 0; i < studentIdsList.length; i++) {
                                if (i % 5 === part) {
                                    studentListTemp.push(studentIdsList[i]);
                                    gcmIdListTemp.push(gcmIdList[i]);
                                }
                            }
                            part++;
                            await kafka.sendNotification(
                                studentIdsList, gcmIdList, notificationPayload,
                            );
                        }
                    }
                }
            }
        }
        job.progress(100);
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
    cron: "30 * * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
