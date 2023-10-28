const moment = require("moment");
const { getMongoClient } = require("../../modules");
const { whatsapp, config } = require("../../modules");

const REMINDER_MSG = "👋Hello Students\n\nAaj yani {{today}} ka 7️⃣ <strong>PM Quiz Contest</strong>  🕖7 baje shuru hone wala hai.\n\nKya aap hain taiyaar❓\nAgar haan toh bas apna <strong>dimag🧠 lagao</strong> aur <strong>maalamaal🤑 ho jao</strong>";
const REGISTER_REMINDER_MSG = "👋Hello Students\nAaj yani {{today}} ka 7️⃣ <strong>PM Quiz Contest 7</strong> 🕖 baje shuru hone wala hai.\n\nToh abhi <strong>register</strong>✅ karein aur <strong>Dhero Prizes</strong>🏆 jeetne ka mauka payein.\n\n<strong>Aaj ke prizes</strong>\n1st -2️⃣5️⃣0️⃣0️⃣\n2nd-1️⃣5️⃣0️⃣0️⃣\n3rd -1️⃣0️⃣0️⃣0️⃣\n\nIss hafte ka mega prize- <strong>mobile phone</strong>📱\n\nContest ki jaankari👇\nhttps://app.doubtnut.com/seven-pm-quiz";
const reminderMsg = {
    action: {
        buttons: [
            { type: "reply", reply: { id: "1", title: "Start 7PM Quiz" } },
            { type: "reply", reply: { id: "2", title: "Home" } },
        ],
    },
    replyType: "BUTTONS",
    mediaUrl: `${config.staticCDN}images/promo_banner_en.webp`,
};
const registerMsg = {
    action: {
        buttons: [
            { type: "reply", reply: { id: "1", title: "Register now 7PMQuiz" } },
            { type: "reply", reply: { id: "2", title: "Home" } },
        ],
    },
    replyType: "BUTTONS",
    mediaUrl: `${config.staticCDN}images/promo_banner_en.webp`,
};

const CAMPAIGN = "QUIZ_REGISTER_645";
let fails = 0; let success = 0;
const db = "whatsappdb";
let count = 0;
// todo correct the table name
const dbSession = "whatsapp_sessions";

async function sendRegisterButton(student, msg) {
    try {
        console.log("##student: ", student.phone, " count: ", count++);
        // sending it as seperate msgs
        whatsapp.sendMediaMsg(CAMPAIGN, student.phone, 0, registerMsg.mediaUrl, "IMAGE", msg, registerMsg.replyType, registerMsg.action);
        success++;
    } catch (e) {
        console.error(e);
        fails++;
    }
}

// todo to be sent to those who have 2-way chat window open
async function getNonRegisteredStudents(client, quizId, msg) {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    console.log("##yesterday: ", yesterday);
    const pipeline = [{
        $match: {
            updatedAt: { $gt: yesterday },
            source: "8400400400",
        },
    }, {
        $project: {
            _id: 0,
            phone: 1,
        },
    }, {
        $lookup: {
            from: "whatsapp_daily_quiz_registers",
            let: { phone: "$phone" },
            pipeline: [{
                $match: {
                    $expr: {
                        $and: [{
                            $eq: ["$quizId", quizId],
                        }, {
                            $eq: ["$phone", "$$phone"],
                        }],
                    },
                },
            }],
            as: "sessionData",
        },
    }, {
        $match: {
            sessionData: { $size: 0 },
        },
    }, {
        $project: {
            _id: 0,
            phone: 1,
        },
    }];
    await new Promise((resolve) => client.collection(dbSession).aggregate(pipeline, { cursor: { batchSize: 50 } }).forEach((student) => {
        sendRegisterButton(student, msg);
    }, (err) => {
        console.error(err);
        resolve();
    }));
}

async function sendReminderMsg(student, msg) {
    try {
        console.log("##student: ", student.phone);
        whatsapp.sendMediaMsg(CAMPAIGN, student.phone, 0, reminderMsg.mediaUrl, "IMAGE", msg, reminderMsg.replyType, reminderMsg.action);
        // whatsapp.sendTextMsg(CAMPAIGN, student.phone, student.studentId, msg, null, null, null, reminderMsg.replyType, reminderMsg.action);
        success++;
    } catch (err) {
        console.error(err);
        fails++;
    }
}

async function getRegisteredStudents(client, quizId, msg) {
    await new Promise((resolve) => client.collection("whatsapp_daily_quiz_registers").find({ quizId }, { projection: { studentId: 1, phone: 1 } })
        .batchSize(50).forEach((student) => {
            sendReminderMsg(student, msg);
        }, (err) => {
            console.error(err);
            resolve();
        }));
}

async function start(job) {
    const today = moment().format("YYYY-MM-DD");
    const client = (await getMongoClient()).db(db);

    const reminder = REMINDER_MSG.replace("{{today}}", moment().format("Do MMM YY"));
    const registerReminder = REGISTER_REMINDER_MSG.replace("{{today}}", moment().format("Do MMM YY"));

    const quizDetails = await client.collection("whatsapp_daily_quizzes").find({ source: "8400400400", quizDate: today }, { projection: { quizId: 1 } }).sort({ _id: 1 }).limit(1)
        .toArray();
    const { quizId } = quizDetails[0];
    await getRegisteredStudents(client, quizId, reminder);
    await job.progress(50);
    await getNonRegisteredStudents(client, quizId, registerReminder);
    console.log("success: ", success, " fails: ", fails);
    await job.progress(100);
}

module.exports.start = start;
module.exports.opts = {
    cron: "45 18 * * *",
    disabled: true,
};
