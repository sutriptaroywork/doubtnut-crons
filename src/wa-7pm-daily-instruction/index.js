const moment = require("moment");
const { getMongoClient } = require("../../modules");
const { whatsapp, gupshup } = require("../../modules");

const instructionPayload = {
    text: "🪧 <strong>Instruction for Doubtnut Daily Quiz Contest</strong> 🪧\n\n1. Iss quiz mein aapko  3️⃣0️⃣ questions puche jaenge.📚\n\n2. Har question❓ mein aapko 4️⃣ <strong>options</strong> diye jaenge.\n\n3. ✅Sahi option choose karne par aapko ➕4️⃣ <strong>marks milenge</strong> and ❌galat choose karne par -1️⃣.\n\n4. Har question mein aapko ek 📍'SKIP'📍 ka button bhi diya jaega, agar aapko kisi sawaal ka jawab nahi aata hai toh aap 📍'SKIP'📍 ka button press karke agle sawaal par jaa sakte hain.\n\n5. Aap har swaal ka jawaab ek hi baar de sakte hain.\n\n6. Skip kiya hua sawaal dubara 🚫 attempt nahi kar sakte hain.\n\n7. Yeh Quiz {{quizEndTime}} par apne aap submit hojaegi.uske baad aap koi bhi answer nahi dae sakte hai\n\n8. Doubtnut Daily Quiz Contest ka result 10:00PM ko bataya jaega.",
    action: {
        buttons: [
            { type: "reply", reply: { id: "1", title: "Start Quiz Contest" } },
        ],
    },
    replyType: "BUTTONS",
};
const CAMPAIGN = "QUIZ_INSTRUCTION_445";
let fails = 0; let success = 0;
const db = "whatsappdb";
let count = 0;
// todo correct the table name
const dbQuizRegistered = "whatsapp_daily_quiz_registers";
async function sendInstruction(student, quizEndTime) {
    try {
        // console.log("##student: ", student.studentId, " phone: ", student.phone, " count: ", count++, " quizEndTime: ", quizEndTime);
        const instructionMsg = instructionPayload.text.replace("{{quizEndTime}}", quizEndTime );
        // console.log("instructionMsg: ", instructionMsg);
        whatsapp.sendTextMsg(CAMPAIGN, student.phone, student.studentId, instructionMsg, null, null, null, instructionPayload.replyType, instructionPayload.action);
        success++;
    } catch (e) {
        console.error(e);
        fails++;
    }
}

// todo to be sent to those who have registered for today's 7pm quiz
async function getRegisteredStudents(client) {
    const today = moment().format("YYYY-MM-DD");
    const cronRunTime = moment().add(5, "hours").add(30, "minutes");
    let batch_id;
    if(cronRunTime.isBefore(moment({ hour: 17, minute: 00 }))){
        batch_id = 1;
    } else if(cronRunTime.isBefore(moment({ hour: 18, minute: 00 }))){
        batch_id = 2;
    } else if(cronRunTime.isBefore(moment({ hour: 19, minute: 00 }))){
        batch_id = 3;
    } else if(cronRunTime.isBefore(moment({ hour: 20, minute: 00 }))){
        batch_id = 4;
    } else{
        batch_id = 5;
    }
    console.log(cronRunTime, batch_id);

    const quizDetails = await client.collection("whatsapp_daily_quizzes").find({ source: "8400400400", quizDate: today, batchId: batch_id }, { quizId: 1, batchId: 1 }).toArray();
    const quizArr = quizDetails.map((x) => x.quizId);
    let quizBatchObj = {};
    for ( let i = 0; i < quizDetails.length; i++ ){
        quizBatchObj[quizDetails[i].quizId] = quizDetails[i].batchId
    }
    // console.log("quizArr: ", quizArr, " quizBatchObj: ", quizBatchObj);
    const quizEndTimeObj = {
        1: "5:30PM",
        2: "6:30PM",
        3: "7:30PM",
        4: "8:30PM",
        5: "9:30PM",
    };
    // console.log(instructionMsg);
    const quizEndTime = quizEndTimeObj[quizBatchObj[quizArr[0]]] || "30 min hone";
    await new Promise((resolve) => client.collection(dbQuizRegistered).find({ quizId: { "$in": quizArr } }, { phone: 1, studentId: 1, quizId: 1 }).batchSize(50).forEach((student) => {
        sendInstruction(student, quizEndTime);
    }, (err) => {
        console.error(err);
        resolve();
    }));
}

async function start(job) {
    const client = (await getMongoClient()).db(db);
    success = 0;fails = 0;
    await getRegisteredStudents(client);
    const cronName = "7pm-dailyInstruction";
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
    cron: "45 16,17,18,19,20 * * *",
};
// micro_test
// today subtract 
// 8400400400 to 