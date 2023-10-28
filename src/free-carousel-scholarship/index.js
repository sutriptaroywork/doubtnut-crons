const moment = require("moment");
const notification = require("../../modules/notification");
const { mysql } = require("../../modules");
const { slack } = require("../../modules");
const { config } = require("../../modules");
const flagr = require("../../modules/flagr");
const helper = require("./helper.js");

async function getNotificationData(locale, subject, chapter, questionId, s_n_id) {
    const title = locale == "hi" ? "1️⃣0️⃣0️⃣% स्कॉलरशिप🏆 जीतने की तैयारी करें" : "Prepare to win 1️⃣0️⃣0️⃣% Scholarship 🏆";
    const message = locale == "hi" ? `✅विषय- ${subject}\n✅अध्याय- ${chapter}` : `✅Subject- ${subject}\n✅Chapter-${chapter}`;
    const imageUrl = "";

    const data = { variant_id: 1, deeplink: `doubtnutapp://live_class?id=${questionId}&page=SCHOLARSHIP_TEST_LF_NOTIF` };
    return {
        notificationData: {
            event: "vip",
            title,
            message,
            image: imageUrl,
            data: JSON.stringify(data),
            s_n_id,
        },
    };
}

function getStudents() {
    const sql = "SELECT se.subject ,se.`type`, se.test_class, se.test_locale , st.student_id , s.gcm_reg_id ,s.locale from scholarship_test st left join scholarship_exam se on st.test_id =se.test_id left join students s on st.student_id =s.student_id where se.is_active =1 and se.publish_time > CURRENT_TIMESTAMP()";
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function start(job) {
    const students = await getStudents();
    let counter = 0;
    const timeNow = moment().add(5, "hours").add(30, "minutes").format("HH:mm:ss");
    const snids = {};
    for (let i = 0; i < students.length; i++) {
        const studentClass = students[i].test_class;
        const locale = students[i].test_locale;
        const studentLocale = students[i].locale == 'hi' ? 'hi' : 'en';
        const flgrData = { body: { capabilities: { dnst_free_carousel: {} }, entityId: students[i].student_id.toString() } };
        const flgrResp = await flagr.getFlagrResp(flgrData);
        if (flgrResp !== undefined && flgrResp !== {} && flgrResp.dnst_free_carousel !== undefined && flgrResp.dnst_free_carousel.enabled && flgrResp.dnst_free_carousel.payload.enabled) {
            const lang = locale === "hi" ? "HINDI" : "ENGLISH";
            const subjectList = [];
            let { subject } = students[i];
            let [exam, mathBio] = subject.split("_");
            if (studentClass >= 11) {
                if (exam == "IIT JEE") {
                    exam = "IIT";
                    mathBio = "MATHS";
                }
                if (exam == "NEET") {
                    mathBio = "BIOLOGY";
                }
                if (exam == "BOARDS" && !mathBio) {
                    subjectList.push("BIOLOGY");
                    mathBio = "MATHS";
                }
            }
            if (!mathBio) {
                mathBio = "MATHS";
            }
            if (timeNow < "12:00:00" && timeNow > "10:59:00") {
                if (studentClass < 11) {
                    continue;
                }
                subjectList.push("PHYSICS");
            } else if (timeNow < "16:00:00" && timeNow > "14:59:00") {
                if (studentClass < 11) {
                    subjectList.push("SCIENCE");
                } else {
                    subjectList.push("CHEMISTRY");
                }
            } else {
                subjectList.push(mathBio);
            }

            const data = await helper.getVideoData(studentClass, subjectList, lang, exam);
            const { chapter } = data[0][0];
            subject = data[0][0].subject;
            const studentsData = [];
            const student = {};
            student.id = students[i].student_id;
            student.gcmId = students[i].gcm_reg_id;
            studentsData.push(student);
            const s_n_id = `${students[i].type}_${studentLocale}_${subject}_${studentClass}_${exam}`;
            if (snids.hasOwnProperty(s_n_id)) {
                snids[s_n_id]++;
            } else {
                snids[s_n_id] = 1;
            }
            const notificationData = await getNotificationData(studentLocale, subject, chapter, data[0][0].question_id, s_n_id);
            const notificationPayload = notificationData.notificationData;
            counter++;
            await notification.sendNotification(studentsData, notificationPayload);
        }
    }
    const blockNew = [];
    for (const key in snids) {
        if (snids.hasOwnProperty(key)) {
            blockNew.push({
                type: "section",
                text: { type: "mrkdwn", text: `Free class snid "${key}" sent` },
            },
            {
                type: "section",
                text: { type: "mrkdwn", text: `*Count*: ${snids[key]}` },
            });
        }
    }
    await slack.sendMessage("#scholarship-notification", blockNew, config.SCHOLARSHIP_SLACK_AUTH);
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "30 5,9,13 * * *", // Daily at 11 am, 3pm and 7pm
};
