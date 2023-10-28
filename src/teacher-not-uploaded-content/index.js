/* eslint-disable no-await-in-loop */
const { mysql } = require("../../modules");
const messages = require("../../modules/messages");

// This is cron is for teachers who have not uploaded any content in last 30 days

function getDetails(limit, offset) {
    const sql = `select distinct t.teacher_id,fname,lname,mobile from classzoo1.teachers t left join classzoo1.course_resources cr 
                 on cr.faculty_id=t.teacher_id left join classzoo1.teachers_resource_upload u on u.course_resource_id=cr.id 
                 where date(u.created_at)>=CURRENT_DATE-29 and t.teacher_id not in (select distinct teacher_id from 
                 classzoo1.teachers_resource_upload) and t.is_verified=1 and cr.vendor_id = 3 order by teacher_id DESC LIMIT ${limit} OFFSET ${offset}`;
    return mysql.pool.query(sql)
        .then((res) => res[0]);
}

async function sendSMS(teacherData) {
    for (const teacher of teacherData) {
        await messages.sendSms({
            phone: teacher.mobile,
            msg: "आदरणीय शिक्षक, हमने देखा है कि आपने अभी तक कोई वीडियो अपलोड नहीं किया है। आप teachers.doubtnut.com पर जा सकते हैं और अभी अपलोड करना शुरू कर सकते हैं। यदि आप किसी समस्या का सामना करते हैं, तो बेझिझक हमें teachers@doubtnut.com पर या +91 7838068076 पर संपर्क करें। कृपया याद रखें, भुगतान के योग्य होने के लिए आपको एक महीने में कम से कम 20 वीडियो अपलोड करने होंगे।",
            locale: "hi",
        });
        console.log("SENT on => ", teacher.mobile);
    }
}

async function start(job) {
    job.progress(10);
    const chunk = 500;
    try {
        for (let i = 0; i < 500000; i += chunk) {
            const teacherData = await getDetails(chunk, i);
            if (!teacherData.length) {
                console.log("No more data");
                break;
            }
            await sendSMS(teacherData);
        }
        job.progress(100);
        return 1;
    } catch (e) {
        await messages.sendSms({
            phone: 9818281365,
            msg: `Dear user, Error occurred in doubtnut studygroup -- teachers-not-uploaded-content, Exception: ${e}`,
            locale: "en",
        });
        throw e;
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "20 10 */2 * *", // On every 2nd day at 10:20 AM
};
