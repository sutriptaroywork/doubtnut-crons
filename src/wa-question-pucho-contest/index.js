const moment = require("moment");
const { redshift, whatsapp, mysql, gupshup, config } = require("../../modules");
const redisClient = require("../../modules/redis");
const _ = require('lodash');

const CAMPAIGN = "WA_QUESTION_PUCHO_CONTEST";
const CAMPAIGN_REWARD = `${CAMPAIGN}_REWARD`;
const weblink = "https://app.doubtnut.com/question-pucho";
const USER_RESULT_MSG = "🎙️<strong>QUESTION PUCHHO</strong> Contest {{today}} Results\n\n👋Hi {{user_name}}\n✅Aapne aaj kul milakar {{number_of_question}} doubts❓ solve karein hain humare Whatsapp number par.\n❗Aapka aaj ke contest mein {{rank}} rank hai.❗";
const LEADERBOARD = "Hello Bacho\n📢📢Aaj ke ({{today}}) <strong>Question Puchho Contest</strong> ke vijeta❗❕\n\n1️⃣.{{name_0}}🥳\nNumber of doubts solved-{{count_0}} \n\n2️⃣.{{name_1}}🎊\nNumber of doubts solved-{{count_1}} \n\n3️⃣.{{name_2}}🎉\nNumber of doubts solved-{{count_2}} \n\n4️⃣.{{name_3}}🕺💃/nNumber of doubts solved-{{count_3}} \n\n5️⃣.{{name_4}}⚡\nNumber of doubts solved-{{count_4}} \n\n6️⃣.{{name_5}}🤩\nNumber of doubts solved-{{count_5}} \n\n7️⃣.{{name_6}}💰\nNumber of doubts solved-{{count_6}} \n\n8️⃣.{{name_7}}💵\nNumber of doubts solved-{{count_7}} \n\n9️⃣.{{name_8}}👨‍🎓👩‍🎓\nNumber of doubts solved-{{count_8}} \n\n1️⃣0️⃣.{{name_9}}😎\nNumber of doubts solved-{{count_9}}";
const REWARD_MSG = "🎙️<strong>Congratulation</strong> ❗❕\n\nAap aaj ke Question Puchho Contest ke Vijeta hain.\nApna reward💵💰 paane ke liye neeche diye gaye link par click karein\nlink-https://app.doubtnut.com/question-pucho";
// const CONTEST_DATE = "<strong>Question Puchho Contest</strong> {{contest_start_date}} se {{contest_end_date}} tak hai.🤩\nToh apne doubts solve karte rahen aur har roz 💵Rs 150/- tak jeet te rahen 🕺💃\n\nContest ki aur jaankari ke liye Click on Link-\nhttps://app.doubtnut.com/question-pucho";
const QA_USER_RESULT_MSG = "Hi {{user_name}}\nAapne aaj total {{number_of_question}} Question puchhe hain.🤩\nPar aapne kisi bhi question ka video solution😅 nahi❌ dekha hai aur apne doubts solve nahi❌ kiye hain.\n\n<strong>Question puchho contest<strong> jeetne🏆 ke liye aapko 3️⃣ simple steps karne hai.\n1️⃣. Whatsapp par question puchhein\n2️⃣. Video solution dekhein\n3️⃣. Apna doubt clear karein";
// const bannerUrl = _.sample([`${config.staticCDN}images/2022/03/28/16-07-08-308-PM_e1.jpg`, `${config.staticCDN}images/2022/03/28/16-07-22-595-PM_e2.jpg`, `${config.staticCDN}images/2022/03/28/16-07-35-668-PM_e3.jpg`]);
const smsPayload = {
    contestDate: "<strong>Question Puchho Contest</strong> {{contest_start_date}} se {{contest_end_date}} tak hai.🤩\nToh apne doubts solve karte rahen aur har roz 💵Rs 150/- tak jeet te rahen 🕺💃\n\nContest ki aur jaankari ke liye Click on Link-\nhttps://app.doubtnut.com/question-pucho",
};
let fails = 0; let success = 0;
// const date = "2021-09-29";
const msg = {
    text: "Neeche home button par click✅ karke aap question❓ puchhna jaari rakh sakte hain ya aap apni knowledge📚 test karne ke liye quiz✏️ khel sakte hain👇",
    footer: "Neeche home button par click karke chat restart karein",
    replyType: "BUTTONS",
    action: {
        buttons: [
            { type: "reply", reply: { id: "1", title: "Home" } },
        ],
    },
};
/* eslint-disable no-await-in-loop */
/**
 * @returns {{student_id: number; mobile: string; name: string; count: number, rank: number}[]}
 */
async function getBannersDnProperty(bucket, name) {
    const sql = "select value from classzoo1.dn_property where bucket = ? and name = ? and is_active =1";
    console.log(sql);
    return mysql.pool.query(sql, [bucket, name]).then((res) => res[0]);
}

async function getStudentDetails(sqlBatch) {
    const sql = `select student_id, student_fname, mobile from classzoo1.students where student_id IN (?)`;
    console.log(sql);
    return mysql.pool.query(sql, [sqlBatch]).then(([res]) => res);
    // const users = await redshift.query(sql).then((res) => res);
    // return users;
}

async function sendFlowSms(studentId, rank, score, studentInfoObj, leaderBoard, thirdMsg) {
    try {
        console.log("###rank: ", rank, " score: ", score, " studentInfoObj: ", studentInfoObj);
        if (!studentInfoObj) {
            throw Error("No data in DB");
        }
        const userResult = USER_RESULT_MSG.replace("{{today}}", moment().format("MMMM Do YYYY")).replace("{{user_name}}", studentInfoObj.name || "student").replace("{{number_of_question}}", score || 0).replace("{{rank}}", rank);
        await whatsapp.sendTextMsg(CAMPAIGN, `91${studentInfoObj.mobile}`, studentId, userResult);
        await whatsapp.sendTextMsg(CAMPAIGN, `91${studentInfoObj.mobile}`, studentId, leaderBoard);
        await whatsapp.sendMediaMsg(CAMPAIGN_REWARD, `91${studentInfoObj.mobile}`, studentId, smsPayload.mediaUrl, "IMAGE", thirdMsg);
        // await whatsapp.sendTextMsg(CAMPAIGN, `91${studentInfoObj.mobile}`, studentId, msg.text, null, null, msg.footer, msg.replyType, msg.action);
        success++;
    } catch (e) {
        console.error(e);
        fails++;
    }
}

async function sendMsgBatch(job, leaderBoard) {
    try {
        const date = moment().add(5, "hours").add(30, "minutes").format("YYYY-MM-DD");
        const totalContestants = await redisClient.zcount(`question_pucho_contest_leaderboard:${date}`, "-inf", "+inf");
        for (let i = 50; i <= totalContestants; i += 100) {
            const sortedStudentBatch = await redisClient.zrevrange(`question_pucho_contest_leaderboard:${date}`, i, i + 99, "WITHSCORES");
            const studentIds = sortedStudentBatch.filter((_element, index) => (index % 2 === 0));
            console.log("##studentIds ", studentIds);
            const studentInfo = await getStudentDetails(studentIds);
            console.log("##studentInfo: ", studentInfo);
            const studentInfoObj = studentInfo.reduce((c, { student_id, student_fname, mobile }) => {
                c[student_id] = {
                    name: student_fname,
                    mobile,
                };
                return c;
            }, {});
            await Promise.all(studentIds.map((x, ind) => sendFlowSms(x, i + ind + 1, sortedStudentBatch[(2 * ind) + 1], studentInfoObj[x], leaderBoard, smsPayload.contestDate)));
            await job.progress(((totalContestants - i - 100) * 100) / totalContestants);
        }
        console.log("##totalContestants: \n", totalContestants);
        return true;
    } catch (e) {
        console.log(e);
    }
}

async function checkQaUserExists(studentId) {
    const date = moment().add(5, "hours").add(30, "minutes").format("YYYY-MM-DD");
    return redisClient.zscore(`question_pucho_contest_leaderboard:${date}`, studentId);
}

async function sendQaMsg(student, leaderBoard, thirdMsg) {
    try {
        // check if the user has received msg already
        redundantUserCheck = await checkQaUserExists(student.student_id);
        if(redundantUserCheck){
            return ;
        }
        // console.log("redundantUserCheck: ", redundantUserCheck, "sendQaMsg mobile: ", student.mobile, "totalCount: ", student.totalcount);
        const qaUserResultMsg = QA_USER_RESULT_MSG.replace("{{user_name}}", student.name || "student").replace("{{number_of_question}}", student.totalcount || 0);
        await whatsapp.sendTextMsg(CAMPAIGN, `91${student.mobile}`, student.student_id, qaUserResultMsg);
        await whatsapp.sendTextMsg(CAMPAIGN, `91${student.mobile}`, student.student_id, leaderBoard);
        await whatsapp.sendMediaMsg(CAMPAIGN, `91${student.mobile}`, student.student_id, smsPayload.mediaUrl, "IMAGE", thirdMsg);
        success++;
    } catch (e) {
        console.error(e);
        fails++;
    }
}

async function getQaStudents(lastStudentId, lowerTimeStamp, upperTimeStamp) {
    const sql = `select A.student_fname as name, A.student_id, A.mobile, B.totalCount from classzoo1.students A join 
    (SELECT DISTINCT student_id, count(*) as totalCount from classzoo1.questions_new
    where doubt ='WHATSAPP' and curtimestamp BETWEEN '${lowerTimeStamp} 22:00:00' and '${upperTimeStamp} 22:00:00' and student_id>${lastStudentId}
    group by student_id) B
    on A.student_id = B.student_id
    order by A.student_id 
    limit 1000`;
    console.log(sql);
    // return mysql.pool.query(sql).then(([res]) => res);
    return redshift.query(sql).then((res) => res);
    // return users;
}

async function sendQaBatch(job, leaderBoard) {
    try {
        const upperTimeStamp = moment().add(5, "hours").add(30, "minutes").format("YYYY-MM-DD");
        const lowerTimeStamp = moment().add(5, "hours").add(30, "minutes").subtract(1, 'day').format("YYYY-MM-DD");
        console.log("upperTimeStamp ", upperTimeStamp, "\nlowerTimeStamp: ", lowerTimeStamp);
        let lastStudentId = 0;
        let students = await getQaStudents(lastStudentId, lowerTimeStamp, upperTimeStamp);
        let batch;
        while (students.length) {
            while (students.length) {
                batch = students.splice(0, 200);
                await Promise.all(batch.map((x) => sendQaMsg(x, leaderBoard, smsPayload.contestDate)));
            }
            lastStudentId = batch.slice(-1)[0].student_id;
            console.log("########################### batch 1000: ", lastStudentId);
            students = await getQaStudents(lastStudentId, lowerTimeStamp, upperTimeStamp);
        }
        
    } catch (e) {
        console.log(e);
    }
}

async function start(job) {
    // send SMS to top50 users and update leaderboard msg
    const bucketImages = await getBannersDnProperty("wa_contest_result", "banners");
    const bannerImage = _.sample(bucketImages[0].value.split("||"));
    smsPayload.mediaUrl = bannerImage ? `${config.staticCDN}images/${bannerImage}` : `${config.staticCDN}images/2022/03/28/16-07-08-308-PM_e1.jpg`;
    const bucketDates = await getBannersDnProperty("wa_qpc_date", "lower_limit_upper_limit");
    const [contestStartDate, contestEndDate] = bucketDates[0].value.split("||");
    const startDate = contestStartDate ? moment(contestStartDate).format("MMMM Do YYYY") : "March 23rd 2022";
    const endDate = contestEndDate ? moment(contestEndDate).format("MMMM Do YYYY") : "April 14th 2022";
    smsPayload.contestDate = smsPayload.contestDate.replace("{{contest_start_date}}", startDate).replace("{{contest_end_date}}", endDate);
    const date = moment().add(5, "hours").add(30, "minutes").format("YYYY-MM-DD");
    const top50 = await redisClient.zrevrange(`question_pucho_contest_leaderboard:${date}`, 0, 49, "WITHSCORES");
    const studentIds = top50.filter((_element, index) => (index % 2 === 0));
    const studentInfo = await getStudentDetails(studentIds);
    const studentInfoObj = await studentInfo.reduce((c, { student_id, student_fname, mobile }) => {
        c[student_id] = {
            name: student_fname,
            mobile,
        };
        return c;
    }, {});
    let leaderBoard = LEADERBOARD.replace("{{today}}", moment().format("MMMM Do YYYY"));
    for (let i = 0; i < 10; i++) {
        const name = studentInfoObj[studentIds[i]] ? studentInfoObj[studentIds[i]].name : "student";
        console.log(`{{name_${i}}}`, " ", name, " ", `{{count_${i}}}`, " ", top50[(2 * i) + 1]);
        leaderBoard = leaderBoard.replace(`{{name_${i}}}`, name || "student").replace(`{{count_${i}}}`, top50[(2 * i) + 1]);
    }
    console.log("##leaderBoard: ", leaderBoard);

    await Promise.all(studentIds.map((x, ind) => sendFlowSms(x, ind + 1, top50[(2 * ind) + 1], studentInfoObj[x], leaderBoard, REWARD_MSG)));
    await job.progress(1);

    // send sms to rest all users
    await sendMsgBatch(job, leaderBoard);
    await job.progress(50);
    await sendQaBatch(job, leaderBoard);
    const cronName = "QA_PUCHO_CONTEST";
    const cronRunDate = moment().add(5, "hours").add(30, "minutes").format("YYYY-MM-DD HH:mm:ss");
    await gupshup.sendSms({
        phone: 9804980804,
        msg: `Doubtnut Cron\n\nCron_name--${cronName}\nRun_date-${cronRunDate}\nUser count-${success}\nCron_start_time-${cronRunDate}`,
    });
    await gupshup.sendSms({
        phone: 8588829810,
        msg: `Doubtnut Cron\n\nCron_name--${cronName}\nRun_date-${cronRunDate}\nUser count-${success}\nCron_start_time-${cronRunDate}`,
    });
    await job.progress(100);
}

module.exports.start = start;
module.exports.opts = {
    cron: "10 22 * * *",
};

