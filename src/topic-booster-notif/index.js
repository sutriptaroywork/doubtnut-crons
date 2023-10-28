const moment = require("moment");
const _ = require("lodash");
const { getMongoClient } = require("../../modules");
const mysql = require("../../modules/mysql");
const fcm = require("../../modules/fcm");
const redisClient = require("../../modules/redis");

function getByKey(key) {
    return redisClient.getAsync(key);
}

class RewardNotifier {
    constructor(client) {
        this.notification_date = moment().format("YYYY-MM-DD");
        this.client = client;
        this.unavailableTopics = [];
        this.availableTopics = [];
    }

    async main(job) {
        console.log("job create notification");
        await job.progress(11);
        const questionAskedStudents = await this.getApplicableStudents();
        console.log(40);
        await job.progress(40);
        let i; let j; let temparray; const
            chunk = 50000;
        for (i = 0, j = questionAskedStudents.length; i < j; i += chunk) {
            temparray = questionAskedStudents.slice(i, i + chunk);
            console.log(temparray, " temp");
            const studentDetails = await this.getStudentDetails(temparray, chunk);
            await this.sendNotification(studentDetails);
            await job.progress(40 + (chunk / 50));
            console.log(40 + (chunk / 50));
        }
        console.log(90);
        await job.progress(90);
    }

    async getApplicableStudents() {
        console.log("executing questions new");
        const sql = `SELECT student_id
                        FROM questions_new q
                        WHERE q.timestamp BETWEEN '${this.notification_date} 00:00:00' AND '${this.notification_date} 23:59:59'
                        GROUP BY q.student_id
                        HAVING COUNT(1) >= 1`;
        const result = await mysql.pool.query(sql)
            .then((res) => res[0]);
        const studentIds = [];
        for (const r of result) {
            studentIds.push(r.student_id);
        }
        return studentIds;
    }

    async getStudentDetails(questionAskedStudents, chunkSize) {
        console.log("executing vvs");
        const sql = `SELECT v.student_id, s.gcm_reg_id, v.question_id, s.locale
                    FROM video_view_stats v join students s on v.student_id =s.student_id
                    WHERE v.student_id in (?) AND v.created_at BETWEEN '${this.notification_date} 00:00:00' AND '${this.notification_date} 23:59:59'
                      AND v.view_from in ('SRP_PLAYLIST','SRP') AND s.is_online >= 871 GROUP BY v.student_id HAVING count(v.student_id) >= 1 LIMIT ?`;
        const result = await mysql.pool.query(sql, [questionAskedStudents, chunkSize])
            .then((res) => res[0]);
        const qids = [];
        for (const r of result) {
            qids.push(r.question_id);
        }
        return { result, qids };
    }

    async sendNotification(studentDetails) {
        const notificationData = {
            event: "topic_booster_game",
            title: "Khelo aur Jeeto!",
            image: null,
            firebase_eventtag: "topic_booster_game",
        };
        const sql = `SELECT question_id, chapter FROM questions_new WHERE question_id in (?) UNION SELECT question_id, 
        chapter from questions WHERE question_id in (?)`;
        const chapterAlias = await mysql.pool.query(sql, [studentDetails.qids, studentDetails.qids])
            .then((res) => res[0]);
        const chapters = {};
        for (const s in chapterAlias) {
            chapters[chapterAlias[s].question_id] = chapterAlias[s].chapter;
        }
        const totalQuestionsForQuiz = 5;
        for (const student of studentDetails.result) {
            const topicKey = `TOPIC_${chapters[student.question_id]}_${totalQuestionsForQuiz}`;
            if (this.unavailableTopics.includes(topicKey)) {
                continue;
            } else if (this.availableTopics.includes(topicKey)) {
                notificationData.message = (student.locale === "hi" ? `खेलें ${chapters[student.question_id]} का क्विज एंड देखें कौन है मास्टर!` : `Khelo doston ke saath ${chapters[student.question_id]} ka quiz aur bano champion!`);
                notificationData.data = JSON.stringify({ qid: student.question_id });
                fcm.sendFcm(student.student_id, student.gcm_reg_id, notificationData, null, null);
                continue;
            }
            const isChapterAliasAllowed = await getByKey(topicKey);
            if (isChapterAliasAllowed) {
                notificationData.message = (student.locale === "hi" ? `खेलें ${chapters[student.question_id]} का क्विज एंड देखें कौन है मास्टर!` : `Khelo doston ke saath ${chapters[student.question_id]} ka quiz aur bano champion!`);
                notificationData.data = JSON.stringify({ qid: student.question_id });
                fcm.sendFcm(student.student_id, student.gcm_reg_id, notificationData, null, null);
                this.availableTopics.push(topicKey);
            } else {
                this.unavailableTopics.push(topicKey);
            }
        }
    }
}

async function start(job) {
    job.progress(10);
    console.log("task started");
    const client = (await getMongoClient()).db("doubtnut");
    const notification = new RewardNotifier(client);
    await notification.main(job);
    await job.progress(100);
    console.log("task completed");
    return { data: "success" };
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 05 18 * * *",
};
