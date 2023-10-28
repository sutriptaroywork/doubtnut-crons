/* eslint-disable no-await-in-loop */
const { redshift, gupshup } = require("../../modules");

/** This cron is to send wa-pitch sms to users in 24 hrs of uninstallation */

const smsContent = {
    en: "Hello Students, \nHumein khed hai ki aapne Doubtnut app uninstall kar diya hai.\nPar vaada hai Doubtnut ka ki aapki padhaai nahi rukne denge.\n\nTo ab aapke har sawaal ka Video Solution milega Whatsapp par bhi\n\n8400400400 par 'Hi' send karein\nClick here to start asking question:https://tiny.doubtnut.com/959s8nb4",
    hi: "हैलो छात्रों, \nहमें खेद है की आपने Doubtnut ऐप अनइंस्टॉल कर दिया है।\nपर वादा है Doubtnut का की आपकी पढ़ाई नहीं रुकने देंगे ।\n\nतो अब आपके हर सवाल का वीडियो सॉल्यूशन मिलेगा Whatsapp पर भी|\n\n 8400400400 पर Hi भेजें\nसवाल पूछने के लिए यहां क्लिक करें :https://tiny.doubtnut.com/959s8nb4",
    other: "Hello Students, \nWe are sorry that you have uninstalled Doubtnut app. \nBut Doubtnut's promise is that your studies will never stop. \nNow you can get Video Solutions for all your questions on Whatsapp also. \n\nSend 'Hi' to 8400400400 \nClick here to ask question:https://tiny.doubtnut.com/959s8nb4",
};

async function uninstalledDaily(lastStudentId) {
    /** Getting students who uninstalled the App in last 1 day */
    const sql = `SELECT distinct B.mobile,B.locale,B.student_id from classzoo1.students AS B
    JOIN
    classzoo1.retarget_student_churn AS C
    ON B.student_id = C.student_id 
    where C.reinstall_timestamp is NULL
    and C.uninstall_timestamp >= CURRENT_DATE - 1 and C.student_id is not null and B.mobile is not null and B.student_id>${lastStudentId}
    Order by B.student_id
    LIMIT 1000`;
    const users = await redshift.query(sql).then((res) => res);
    return users;
}

async function sendSmsStudent(student, res) {
    try {
        const allowedLocales = ["hi", "en"];
        let locale = "other";
        if (allowedLocales.includes(student.locale)) {
            locale = student.locale;
        }
        console.log("locale: ", locale, " phone: ", student.mobile);
        gupshup.sendSms({
            phone: student.mobile,
            msg: smsContent[locale],
            locale,
        });
        res.totalSent += 1;
    } catch (e) {
        res.totalNotSent += 1;
    }
}

async function start(job) {
    const res = { totalSent: 0, totalNotSent: 0 };
    console.log("students who uninstalled the app in last 24 hours");
    let lastStudentId = 0;
    let students = await uninstalledDaily(lastStudentId);
    let batch;
    while (students.length) {
        while (students.length) {
            batch = students.splice(0, 200);
            await Promise.all(batch.map((x) => sendSmsStudent(x, res)));
        }
        lastStudentId = batch.slice(-1)[0].student_id;
        console.log("########################### batch 1000: ", lastStudentId);
        students = await uninstalledDaily(lastStudentId);
    }
    await job.progress(90);
    const cronName = "uninstall-wa-pitch-d0";
    const today = new Date().toISOString().slice(0, 10);
    await job.progress(100);
    console.log("Total msg sent", res.totalSent, "\nTotal msg not sent: ", res.totalNotSent);
    return res;
}

module.exports.start = start;
module.exports.opts = {
    cron: "33 19 * * *",
};
