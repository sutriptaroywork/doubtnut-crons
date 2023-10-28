/* eslint-disable no-await-in-loop */
const { redshift, gupshup, config, mysql, whatsapp } = require("../../modules");
const moment = require('moment');
/** This cron is to send msg to push students for wa-QA **/
const smsPayload = {
    text: "Kya aapke aaj ke sabhi doubts clear ho gaye hain🤔\nNahi !🤯\nTo abhi clear karein <strong>Doubtnut</strong> ke sath\n\nSubah ke doubts to clear ho gaye hain...\nTo chaliye ab dophar ke doubts clear karte hain📚📚",
    footer: "Neeche Ask Question button ko click kijiye aur doubt puchiye",
    mediaUrl: `${config.staticCDN}images/2021_11_13_wa_QA_publicity_en.jpeg`,
    replyType: "BUTTONS",
    action: {
        buttons: [
            { type: "reply", reply: { id: "1", title: "Ask a Question" } },
            { type: "reply", reply: { id: "2", title: "Home" } },
        ],
    },
};
const CAMPAIGN = "QA_INACTIVITY_NUDGE";
async function getInactiveUsers(lastStudentId) {
    const sql = `select distinct a.student_id,s.mobile from
    (select date(curtimestamp+ interval '330 minute') as date,
            extract (hour from (curtimestamp+ interval '330 minute')) as hour,
     student_id from
    classzoo1.questions_new
    where date(curtimestamp+ interval '330 minute')=CURRENT_DATE and
    doubt='WHATSAPP' and student_id>${lastStudentId}
    group by 1,2,3) as a
    left join
    (select date(curtimestamp+ interval '330 minute') as date,
            extract (hour from (curtimestamp+ interval '330 minute')) as hour,
     student_id from
    classzoo1.questions_new
    where date(curtimestamp+ interval '330 minute')=CURRENT_DATE and
    doubt='WHATSAPP'
    group by 1,2,3) as b on a.student_id=b.student_id and a.date=b.date and (b.hour between 12 and 15)
    left join classzoo1.students s on s.student_id=a.student_id
    where a.hour<12
    and b.student_id is NULL
    Order by a.student_id
    LIMIT 1000`;
    const users = await redshift.query(sql).then((res) => res)
    // const users = await mysql.pool.query(sql).then(([res]) => res);
    return users;
}

async function sendMsgStudent(student, res) {
    try {
        // console.log("phone: ", student.mobile);
        await whatsapp.sendMediaMsg(CAMPAIGN, `91${student.mobile}`, 0, smsPayload.mediaUrl, "IMAGE", smsPayload.text, smsPayload.replyType, smsPayload.action, smsPayload.footer);
        res.totalSent += 1;
    } catch (e) {
        res.totalNotSent += 1;
    }
}

async function start(job) {
    const res = { totalSent: 0, totalNotSent: 0 };
    try {
        let lastStudentId = 0;
        let students = await getInactiveUsers(lastStudentId);
        let batch;
        while (students.length) {
            while (students.length) {
                batch = students.splice(0, 200);
                await Promise.all(batch.map((x) => sendMsgStudent(x, res)));
            }
            lastStudentId = batch.slice(-1)[0].student_id;
            students = await getInactiveUsers(lastStudentId);
        }
        const cronName = "QA_INACTIVITY_NUDGE";
        const cronRunDate = moment().add(5, "hours").add(30, "minutes").format("YYYY-MM-DD HH:mm:ss");
        await gupshup.sendSms({
            phone: 9804980804,
            msg: `Doubtnut Cron\n\nCron_name--${cronName}\nRun_date-${cronRunDate}\nUser count-${res.totalSent}\nCron_start_time-${cronRunDate}`,
        });
        await gupshup.sendSms({
            phone: 8588829810,
            msg: `Doubtnut Cron\n\nCron_name--${cronName}\nRun_date-${cronRunDate}\nUser count-${res.totalSent}\nCron_start_time-${cronRunDate}`,
        });
        console.log("success: ", res.totalSent, " fails: ", res.totalNotSent);
        return res;
    } catch (e) {
        console.log(e);
        return { err: e, res };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "10 15 * * *",
};
