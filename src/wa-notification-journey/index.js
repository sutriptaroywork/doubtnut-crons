/* eslint-disable eqeqeq */
/* eslint-disable no-await-in-loop */
const moment = require("moment");
const _ = require("lodash");
const {
    redshift, slack, config, mysql, whatsapp,
} = require("../../modules");
/** This cron is to send msg to push students for wa-Notification-journey * */
const CAMPAIGN = "WA_NOTIFICATION_JOURNEY";
const smsPayload = {
    text: "Kya aapke aaj ke sabhi doubts clear ho gaye hain🤔\nNahi !🤯\nTo abhi clear karein <strong>Doubtnut</strong> ke sath\n\nSubah ke doubts to clear ho gaye hain...\nTo chaliye ab dophar ke doubts clear karte hain📚📚",
    footer: "Click clear photo, Crop 1 Question, Send, Get video solution",
    // mediaUrl: _.sample([`${config.staticCDN}images/2022/03/28/16-07-08-308-PM_e1.jpg`, `${config.staticCDN}images/2022/03/28/16-07-22-595-PM_e2.jpg`, `${config.staticCDN}images/2022/03/28/16-07-35-668-PM_e3.jpg`]),
    replyType: "BUTTONS",
    action: {
        buttons: [
            { type: "reply", reply: { id: "1", title: "Ask a Question" } },
            { type: "reply", reply: { id: "2", title: "Home" } },
        ],
    },
};
const queriesArr = [
    `select s.mobile, s.student_fname, temp.student_id, temp.count1, temp.count2
from classzoo1.students s
join 
(select q.student_id,
count(case when q.date=CURRENT_DATE then q.question_id end) as count1,
count(case when q.date=CURRENT_DATE-1 and q.hr<(extract (hour from (current_timestamp+ interval '330 minute')))
then q.question_id end) as count2
from
(select distinct student_id from
(
select student_id ,min(datediff(minute,(curtimestamp + interval '330 minute'),current_timestamp::timestamp+ interval '330 minute')) as mi_min from classzoo1.questions_new qn 
where doubt ='WHATSAPP'
group by 1
)
where mi_min between 60 and 120) as w
join
(select date(curtimestamp+ interval '330 minute') as date,
extract (hour from (curtimestamp+ interval '330 minute')) as hr,
student_id,question_id from
classzoo1.questions_new 
where doubt='WHATSAPP'
group by 1,2,3,4) q on q.student_id=w.student_id 
group by 1) as temp
ON temp.student_id = s.student_id
where s.mobile is not null`,
    `select s.mobile, s.student_fname, temp.student_id, temp.count1, temp.count2
from classzoo1.students s
right join 
(select q.student_id,
count(case when week=DATE_PART(week,CURRENT_DATE) then question_id end) as count1,
count(case when week=(DATE_PART(week,CURRENT_DATE)-1) 
and cts<((current_timestamp)-interval '7 days')
then question_id end) as count2
from
(select distinct student_id from
(
select student_id ,min(datediff(minute,(curtimestamp + interval '330 minute'),current_timestamp::timestamp+ interval '330 minute' )) as mi_min from classzoo1.questions_new qn 
where doubt ='WHATSAPP'
group by 1
)
where mi_min between 60 and 120) as w
join
(select curtimestamp+ interval '330 minute' as cts,
DATE_PART(week,date(curtimestamp+ interval '330 minute')) as week,
datepart(dow,date(curtimestamp+ interval '330 minute')) as weekday,
extract (hour from (curtimestamp+ interval '330 minute')) as hr,
student_id,question_id from
classzoo1.questions_new 
where doubt='WHATSAPP'
group by 1,2,3,4,5,6) q on q.student_id=w.student_id 
group by 1) as temp 
ON temp.student_id = s.student_id
where s.mobile is not null`,
    `select w.student_id,a.mobile,a."count1",
a.max_ques-a.ques "count2"
from
(select q.student_id,s.mobile,ques,q.date,q.rank, w.count-rank as "count1",
q2.max_ques
from
(select student_id,ques,date,rank() over (ORDER BY ques desc) as rank
from
(select date(curtimestamp+ interval '330 minute') as date,
student_id,count(question_id) as ques from
classzoo1.questions_new
where date(curtimestamp+ interval '330 minute')=CURRENT_DATE and
doubt='WHATSAPP'
group by 1,2
order by 3 desc)
as a) as q
left join
(
select date,max(ques) as max_ques from
(SELECT to_date(created_at+ interval '330 minutes','YYYY|MM|DD') as date,student_id,count(distinct parent_id) as ques from classzoo1.video_view_stats where
source IN ('WHA', 'WHA_new') and parent_id<>'0'  and engage_time>20
and to_date(created_at + interval '330 minutes','YYYY|MM|DD')=current_date
group by 1,2 order by 3 desc
)
group by 1) as q2 on q.date=q2.date
left join
(SELECT date(createdat+ interval '330 minute') as date ,COUNT(distinct phone) from whatsappdb.whatsapp_events
where eventtype='MO' and source='8400400400'
and date(createdat+ interval '330 minute')=CURRENT_DATE
group by 1) as w on w.date=q.date
left join classzoo1.students s on s.student_id=q.student_id
) as a
left join
(select distinct student_id from
(
select student_id ,min(datediff(minute,(curtimestamp + interval '330 minute'),current_timestamp::timestamp+ interval '330 minute')) as mi_min from classzoo1.questions_new qn
where doubt ='WHATSAPP'
group by 1
)
where mi_min between 60 and 120) as w on w.student_id=a.student_id
where a.mobile is not null
group by 1,2,3,4
having w.student_id is not null
order by 1`,
    `select w.student_id,a.mobile,a."count1",
a.max_ques-a.ques "count2"
from
(
select q.student_id,s.mobile,ques,q.week,q.rank, w.count-rank as "count1",
q2.max_ques
from
(select a.student_id,ques,week,rank() over (ORDER BY ques desc) as rank
from
(select DATE_PART(week,date(curtimestamp+ interval '330 minute')) as week,
student_id,count(question_id) as ques from
classzoo1.questions_new
where DATE_PART(week,date(curtimestamp+ interval '330 minute'))=DATE_PART(week,CURRENT_DATE)
and doubt='WHATSAPP'
group by 1,2
order by 3 desc) as a) as q
left join
(
select week,max(ques) as max_ques from
(SELECT DATE_PART(week,date(created_at+ interval '330 minute')) as week,student_id,count(distinct parent_id) as ques from classzoo1.video_view_stats where
source IN ('WHA', 'WHA_new') and parent_id<>'0'  and engage_time>20
and DATE_PART(week,date(created_at + interval '330 minute'))=DATE_PART(week,CURRENT_DATE)
group by 1,2 order by 3 desc
)
group by 1) as q2 on q.week=q2.week
left join
(SELECT DATE_PART(week,date(createdat+ interval '330 minute')) as week,COUNT(distinct phone) from whatsappdb.whatsapp_events
where eventtype='MO' and source='8400400400'
and DATE_PART(week,date(createdat+ interval '330 minute'))=DATE_PART(week,CURRENT_DATE)
group by 1) as w on w.week=q.week
left join classzoo1.students s on s.student_id=q.student_id) as a
left join
(select distinct student_id from
(
select student_id ,min(datediff(minute,(curtimestamp + interval '330 minute'),current_timestamp::timestamp+ interval '330 minute')) as mi_min from classzoo1.questions_new qn
where doubt ='WHATSAPP'
group by 1
)
where mi_min between 60 and 120) as w on w.student_id=a.student_id
where a.mobile is not null
group by 1,2,3,4
having w.student_id is not null
order by 2`];

const textObj = {
    0: {
        text1: _.sample(["<strong><i>Kya hua aaj chutti par ho kya</strong></i> {{user_name}}🤔\n\nKal abhi tak {{count2}} questions puchhe the aur aaj bas {{count1}} questions🤷\n<strong><i>Exams mein top karna hai toh DOUBTS solve karna jaruri hai</strong></i> 😃", "<strong><i>Saare DOUBTS kal hi puchh kar khatam kar diye kya?</strong></i> 🤔\n\nKal abhi tak {{count2}} questions puchhe the aur aaj bas {{count1}} questions🤷\n<strong><i>Exams mein Top karna hai toh DOUBTS solve karna jaruri hai</strong></i> 😃"]),
        text2: "<strong><i>Bahut ache {{user_name}}</strong></i> 😃👍\n\nAapne kal abhi tak jitne questions puchhe the utne aaj bhi puchh chuke hain.\nAur questions puchho aur khud se aage nikalo, <strong><i>Tabhi toh Exams mein top karoge</strong></i>",
        text3: "<strong><i>Lagta hai aap EXAMS mein top karne wale hain.</strong></i> 😃\n\nAaj abhi tak aapne kal se {{count3}} questions zyada puchhe hain.👍",
    },
    1: {
        text1: _.sample(["<strong><i>Kya hua iss week chutti par ho kya {{user_name}}</strong></i> 🤔\n\nPichhle week abhi tak {{count2}} questions puchhe the aur iss week abhi tak bas {{count1}} questions\n<strong><i>Exams mein top karna hai toh DOUBTS solve karna jaruri hai.</strong></i>",
            "<strong><i>Saare DOUBTS Pichhle week hi puchh kar khatam kar diye kya?</strong></i> 🤔\n\nPichhle week abhi tak {{count2}} question puchhe the aur iss week abhi tak bas {{count1}} questions.\n<strong><i>Exams mein Top karna hai toh DOUBTS solve karna jaruri hai</strong></i> 😃"]),
        text2: "<strong><i>Bahut ache {{user_name}}</strong></i> 😃👍\n\nAapne pichhle hafte abhi tak jitne questions puchhe the utne iss week bhi puchh chuke hain.👍\nAur questions puchho aur khud se aage nikalo, <strong><i>Tabhi toh Exams mein Top karoge</strong></i> 😃",
        text3: "<strong><i>Lagta hai aap EXAMS mein top karne wale hain.</strong></i> 😃\n\nPeechle week abhi tak aapne iss week se {{count3}} questions zyada puchhe hain.👍",
    },
    2: {
        text1: _.sample(["Aapne aaj {{count1}} students se zyada questions puchhe hain 👌\n\nExam mein ache marks 💯 laane ke liye aur questions puchte rahe aur doubts solve karte rahe 👍😀",
            "Aap aajke highest number of doubt puchhne wale user se bas {{count2}} Doubts peeche hain 😇 \n\n<strong>Jaldi Apne saare doubts clear karlo ✅</strong>, Kahi koi aur aapse aage na nikal jaye Exams mein ✌👨🏼‍🤝‍👨🏻",
            "Doubtnut app par har roz <strong>lakhon bachhe 👩🏿‍🤝‍🧑🏾👨🏽‍🤝‍👨🏽doubts puchte hain</strong>.\n\nPar sabhi bachhe jo exams mein top karte hain woh doubtnut par har roz lakhon doubts puchte hain ✅\n\nKya aap mein hai woh toppers wali baat 😀?\n\nAgar haan toh <strong>puchho doubts</strong> aur karo Exams mein top 💯👍🏻",
            "Sochne se jawab nahi mila? \n<strong>Toh Puchne se milega 😀👍🏻</strong>\n\n Aap aajke highest number of doubt puchhne wale user se bas {{count2}} Doubts peeche hain 😇\n<strong>Doubtnut pe puchiye sawaal aur aage badhiye</strong> competition mein💯👍🏻",
            "<strong>Problem solving aasaan hoga Doubtnut pe!😀</strong>\n\n<strong>Jo Doubts abhi tak aya hey</strong>, wo puchiye aur competition mein aage rahein!👍🏻👨🏽‍🤝‍👨🏽👩🏿‍🤝‍🧑🏾"]),
    },
    3: {
        text1: _.sample(["Aapne iss hafte {{count1}} students se zyada questions puchhe hain 👌😀\n\nExam mein ache marks laane ke liye <strong>aur questions puchte rahen</strong> aur doubts solve karte rahen ✅📝",
            "<strong>Doubtnut app par har hafte lakhon bachhe 👩🏿‍🤝‍🧑🏾👨🏽‍🤝‍👨🏽doubts puchte hain.</strong>\n\nPar sabhi bache jo exams mein Top karte hai 🥇 woh doubtnut par har hafte lakhon doubts puchte hain 😀\n<strong>Kya aap mein hai woh toppers wali baat?</strong>\n\nAgar haan toh puchho doubts aur<strong>karo Exams mein Top</strong>💯✅",
            "<strong>Problem solving aasaan hoga Doubtnut pe!😀</strong>\n\n<strong>Jo Doubts abhi tak aya hey</strong>, wo puchiye aur competition mein aage rahein!👍🏻👨🏽‍🤝‍👨🏽👩🏿‍🤝‍🧑🏾"]),
    },
};
async function getTextKey(student, rand) {
    if (rand == 0 || rand == 1) {
        if (student.count2 > student.count1) {
            return "1";
        }
        return (student.count1 == student.count2) ? "2" : "3";
    }
    return "1";
}
async function getActiveUsers(rand) {
    const sql = queriesArr[rand];
    console.log("rand: ", rand, "sql: ", sql);
    return redshift.query(sql).then((res) => res);
}
async function sendMsgStudent(student, rand, res) {
    try {
        // different msgs are picked here for each user based on their qaCount on day0 and day1
        console.log("phone: ", student.mobile);
        let textKey = await getTextKey(student, rand);
        textKey = `text${textKey}`;
        const text = textObj[rand][textKey].replace("{{user_name}}", student.student_fname || "Student").replace("{{count1}}", student.count1 || "0").replace("{{count2}}", student.count2 || "0").replace("{{count3}}", student.count1 - student.count2);
        console.log("textKey: ", textKey, "\ntext:", text);
        await whatsapp.sendMediaMsg(CAMPAIGN, `91${student.mobile}`, 0, smsPayload.mediaUrl, "IMAGE", text || smsPayload.text, smsPayload.replyType, smsPayload.action, smsPayload.footer, moment().add(1, "day").format("YYYY-MM-DD"));
        res.totalSent += 1;
    } catch (e) {
        res.totalNotSent += 1;
    }
}

async function sendCronStats(res) {
    const cronName = CAMPAIGN;
    const cronRunDate = moment().add(5, "hours").add(30, "minutes").format("YYYY-MM-DD HH:mm:ss");
    const userCount = `O: ${res.totalSent}  E: ${res.totalNotSent}`;
    const blockNew = [];
    blockNew.push({
        type: "section",
        text: {
            type: "mrkdwn",
            text: `Doubtnut Cron ran successfully\n\nCron name - ${cronName}\nRun date - ${cronRunDate}\nUser count - ${userCount}\nCron start time - ${cronRunDate}`,
        },
    });
    await slack.sendMessage("#wa-bot-changes-courses-prepurchase", blockNew, config.waAutobotSlackAuth);
}

async function getBannersDnProperty(bucket, name) {
    const sql = "select value from classzoo1.dn_property where bucket = ? and name = ? and is_active =1";
    console.log(sql);
    return mysql.pool.query(sql, [bucket, name]).then((res) => res[0]);
}

async function start(job) {
    const res = { totalSent: 0, totalNotSent: 0 };
    try {
        job.progress(0);
        const bucketImages = await getBannersDnProperty("wa_hourly_notification", "banners");
        const bannerImage = _.sample(bucketImages[0].value.split("||"));
        smsPayload.mediaUrl = bannerImage ? `${config.staticCDN}images/${bannerImage}` : `${config.staticCDN}images/2022/03/28/16-07-08-308-PM_e1.jpg`;
        const rand = _.sample([0, 1, 2, 3]);
        const students = await getActiveUsers(rand);
        let batch;
        while (students.length) {
            batch = students.splice(0, 200);
            await Promise.all(batch.map((x) => sendMsgStudent(x, rand, res)));
        }
        await sendCronStats(res);
        console.log("success: ", res.totalSent, " fails: ", res.totalNotSent);
        job.progress(100);
        return res;
    } catch (e) {
        console.log(e);
        const blockNew = [];
        blockNew.push({
            type: "section",
            text: {
                type: "mrkdwn",
                text: "ALERT!!! Exception in sending Whatsapp Notification",
            },
        });
        await slack.sendMessage("#wa-bot-changes-courses-prepurchase", blockNew, config.waAutobotSlackAuth);
        return { err: e, res };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "10 15 * * *",
};
