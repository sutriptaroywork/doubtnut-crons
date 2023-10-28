/* eslint-disable prefer-const */
/* eslint-disable no-await-in-loop */
const moment = require("moment");
const notification = require("../../modules/notification");
const { mysql } = require("../../modules");
const { redshift } = require("../../modules");
const helper = require("./helper");
const redisClient = require("../../modules/redis");

async function getNotificationData(title, message, deeplink, imageUrl) {
    const data = { variant_id: 1, deeplink };
    return {
        notificationData: {
            event: "vip",
            title,
            message,
            image: imageUrl,
            data: JSON.stringify(data),
        },
    };
}

async function getTopVideosByCcmId() {
    const sql = `select * from
    (
    select a.*,ccm.id,
    rank() over (partition by ccm.id order by view_count desc ) as view_rank
    from 
    (
    select distinct vvs.question_id ,count(distinct vvs.view_id) as view_count
    from classzoo1.video_view_stats vvs 
    group by 1
    ) a
    join classzoo1.questions q
    on q.question_id = a.question_id 
    join classzoo1.studentid_package_mapping_new spm 
    on spm.student_id = q.student_id 
    join classzoo1.class_course_mapping ccm 
    on ccm.course = spm.target_group
    where spm.package like '%NCERT%' and ccm.is_active = 1
    ) b
    where view_rank=1`;
    return redshift.query(sql);
}

async function getStudents(limit, offset) {
    const sql = "select scm.student_id, scm.ccm_id, s.gcm_reg_id, s.locale from classzoo1.student_course_mapping scm join classzoo1.students s on s.student_id = scm.student_id join classzoo1.class_course_mapping ccm on scm.ccm_id = ccm.id where ccm.category = 'board' and ccm.is_active = 1 limit ? offset ?";
    return mysql.pool.query(sql, [limit, offset]).then((res) => res[0]);
}

async function getnotifText() {
    const sql = "select value from dn_property where bucket = 'ncert_notification' and name = 'Title and Message' and is_active = 1";
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getLibarayQuestionDataByQuestionId(qid) {
    const sql = "SELECT doubt, student_id, class, subject, chapter FROM questions WHERE question_id = ?";
    return mysql.pool.query(sql, [qid]).then((res) => res[0]);
}

async function writePlayListRedis(ccmId, playListData) {
    await redisClient.setAsync(`NCERT_PLAYLIST_${ccmId}`, JSON.stringify(playListData), "Ex", 60 * 60 * 25);
}

async function createPlayList(questionData, ccmId) {
    const playListData = await helper.rescursiveList(questionData, 1, 10, []);
    await writePlayListRedis(ccmId, playListData);
}

function getPlaylistDeeplink(questionData, id) {
    return `doubtnutapp://playlist?playlist_id=NCERTNOTIF_${id}&playlist_title=${questionData.subject.replace("/ /g", "%20")}%20${questionData.chapter.replace("/ /g", "%20")}&is_last=1`;
}

async function start(job) {
    let j = 0;
    const QUERYLIMIT = 40000;
    let students = await getStudents(QUERYLIMIT, j);
    j += QUERYLIMIT;
    const topVideos = await getTopVideosByCcmId();

    const playlistByCcmId = {};
    for (let i = 0; i < topVideos.length; i++) {
        const questionData = await getLibarayQuestionDataByQuestionId(topVideos[i].question_id);
        await createPlayList(questionData[0], topVideos[i].id);
        playlistByCcmId[topVideos[i].id] = { deeplink: getPlaylistDeeplink(questionData[0], topVideos[i].id), chapter: questionData[0].chapter };
    }
    const notifTextList = await getnotifText();
    const index = (moment().diff("2022-05-07", "days")) % notifTextList.length;
    while (students.length) {
        for (let i = 0; i < students.length; i++) {
            if (playlistByCcmId.hasOwnProperty(students[i].ccm_id)) {
                let titleAndMessage = notifTextList[index].value.split("||");
                const studentsData = [];
                const student = {};
                student.id = students[i].student_id;
                student.gcmId = students[i].gcm_reg_id;
                const { locale } = students[i];
                let imageUrl;
                if (locale == "en") {
                    titleAndMessage = titleAndMessage[0];
                    imageUrl = 'https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/B9FE5D3A-6249-01A3-9467-9A4AC260B203.webp';
                } else if (locale == "hi") {
                    titleAndMessage = titleAndMessage[1];
                    imageUrl = 'https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/1F91350D-CDE9-AF0E-E0F5-E950D49EAB20.webp';
                } else {
                    titleAndMessage = titleAndMessage[2];
                    imageUrl = 'https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/FCDDEF03-F606-1F26-06E2-431243CE4075.webp';
                }
                let [title, message] = titleAndMessage.split("|");
                title = title.replace("<>", playlistByCcmId[students[i].ccm_id].chapter);
                studentsData.push(student);
                const { deeplink } = playlistByCcmId[students[i].ccm_id];
                const notificationData = await getNotificationData(title, message, deeplink, imageUrl);
                const notificationPayload = notificationData.notificationData;
                await notification.sendNotification(studentsData, notificationPayload);
            }
        }
        students = await getStudents(QUERYLIMIT, j);
        j += QUERYLIMIT;
    }
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "30 11 * * *", // Daily at 5pm
};
