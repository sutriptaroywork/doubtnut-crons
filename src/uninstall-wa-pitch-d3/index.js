/* eslint-disable no-await-in-loop */
const { redshift, gupshup } = require("../../modules");

/** This cron is to send wa-pitch sms to users on the day3 of uninstallation and asked 0 question on whatsapp in this interval */

const smsContent = {
    en: "Hello Students,\nKya aapke dimaag mein doubts hain?\nAgar haan to Doubtnut whatsapp par puchho na\nMilega har sawaal ka video solution wo bhi sirf 10 seconds.\n\n8400400400 par 'Hi' send karein\nClick here to start asking question:https://tiny.doubtnut.com/959s8nb4",
    hi: "हैलो छात्रों, \nक्या आपके दिमाग में डाउट्स हैं? \nअगर हां तो Doubtnut whatsapp पर पूछो ना । \nमिलेगा हर सवाल का वीडियो सॉल्यूशन वो भी सिर्फ 10 सेकंड में । \n\n8400400400 पर 'Hi' भेजें \nसवाल पूछने के लिए यहां क्लिक करें:https://tiny.doubtnut.com/959s8nb4",
    other: "Hello Students,\nDo you have doubts in your mind?\nIf yes, then ask on Doubtnut WhatsApp. \nYou will get Video Solutions of every question in just 10 seconds.\n \nSend 'Hi' to 8400400400 \nClick here to ask a question:https://tiny.doubtnut.com/959s8nb4",
};

async function uninstalledInactiveWa(lastStudentId) {
    // Getting students who uninstalled the App 3 days ago and asked 0 question on whatsapp in this interval
    const sql = `select distinct s.mobile,s.student_id, s.locale
    from classzoo1.students s
    where s.student_id in (
    select r.student_id
    from classzoo1.retarget_student_churn r
    where date(r.uninstall_timestamp)=CURRENT_DATE-3 and r.reinstall_timestamp is null and r.student_id>${lastStudentId} and r.student_id not in (select student_id from classzoo1.questions_new q where date(q.curtimestamp + interval '330 minutes')>=CURRENT_DATE-2 and q.doubt IN ('WHATSAPP', 'WHATSAPP_NT'))
    )
    order by s.student_id
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
    try {
        console.log("students who uninstalled the app 3 days ago and no WA activity");
        let lastStudentId = 0;
        let students = await uninstalledInactiveWa(lastStudentId);
        let batch;
        while (students.length) {
            while (students.length) {
                batch = students.splice(0, 200);
                await Promise.all(batch.map((x) => sendSmsStudent(x, res)));
            }
            lastStudentId = batch.slice(-1)[0].student_id;
            students = await uninstalledInactiveWa(lastStudentId);
        }
        const cronName = "uninstall-wa-pitch-d3";
        const today = new Date().toISOString().slice(0, 10);
        await job.progress(100);
        console.log("Total msg sent", res.totalSent, "\nTotal msg not sent: ", res.totalNotSent);
        return res;
    } catch (e) {
        console.log(e);
        return { err: e, res };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "37 19 * * *",
};
