/* eslint-disable no-await-in-loop */
const { mysql } = require("../../modules");
const messages = require("../../modules/messages");

// This is cron is for teachers who have uploaded any content in last 30 days

function getDetails(limit, offset) {
    const sql = `select distinct t.teacher_id,fname,lname,mobile from classzoo1.teachers t left join classzoo1.course_resources cr 
                 on cr.faculty_id=t.teacher_id left join classzoo1.teachers_resource_upload u on u.course_resource_id=cr.id 
                 where date(u.created_at)>=CURRENT_DATE-29 and t.is_verified=1 and u.is_uploaded=1 and cr.vendor_id = 3 order by teacher_id DESC LIMIT ${limit} OFFSET ${offset}`;
    return mysql.pool.query(sql)
        .then((res) => res[0]);
}

async function sendSMS(teacherData) {
    for (const teacher of teacherData) {
        await messages.sendSms({
            phone: teacher.mobile,
            msg: "आदरणीय शिक्षक, हमारे शिक्षक मंच पर वीडियो अपलोड करने के लिए धन्यवाद! हम आपसे अनुरोध करते हैं कि छात्रों के बीच अपनी पहुंच और लोकप्रियता बढ़ाने के लिए और वीडियो अपलोड करते रहें। आपको \"Featured Teacher of the Week\" बनने और अतिरिक्त पुरस्कार राशि जीतने का मौका भी मिल सकता है। किसी भी प्रश्न के लिए, बेझिझक हमें teachers@doubtnut.com पर संपर्क करें।",
            locale: "hi",
        });
        console.log("SENT on => ", teacher.mobile);
    }
}

async function start(job) {
    try {
        job.progress(10);
        const chunk = 500;
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
            msg: `Dear user, Error occurred in doubtnut studygroup -- teachers-uploaded-content, Exception: ${e}`,
            locale: "en",
        });
        throw e;
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "10 10 */2 * *", // On every 2nd day at 10:10AM
};
