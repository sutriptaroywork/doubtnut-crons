/* eslint-disable no-await-in-loop */
const moment = require("moment");
const { whatsapp, gupshup } = require("../../modules");
const { getMongoClient, config } = require("../../modules");

const CAMPAIGN = "WA_7PM_RESULT";
const db = "whatsappdb";
let count = 0;
const QUES_SOLUTION = "✅Correct Answer: {{correct_option}}\n👉Your Answer: {{user_choice}}\n\n🎙️Video solution: \n{{link}}";

function getPipeline(quizId) {
    const pipeline = [{
        $match: {
            quizId,
        },
    }, {
        $group: {
            _id: { studentId: "$studentId", questionNumber: "$questionNumber" },
            selectedOption: { $first: "$selectedOption" },
            phone: { $first: "$phone" },
        },
    }, {
        $group: {
            _id: "$_id.studentId",
            phone: { $first: "$phone" },
            questionData: {
                $push: {
                    questionNumber: "$_id.questionNumber",
                    selectedOption: "$selectedOption",
                },
            },
        },
    }, {
        $project: {
            phone: 1,
            // questionCount: { $size: "$questionData" },
            questionData: 1,
        },
    }];
    return pipeline;
}

async function sendSolution(student, quizDetails) {
    try {
        console.log("student: ", student.phone, " count: ", count++);
        const quesSolution = QUES_SOLUTION;
        for (let i = 0; i < 10 && i < student.questionData.length; i++) {
            const selectedOption = student.questionData[i].selectedOption || "N/A";
            const { questionNumber } = student.questionData[i];

            const quizResult = quesSolution.replace("{{question_number}}", i + 1)
                .replace("{{correct_option}}", quizDetails[0].questions[questionNumber].correctOptions.join(","))
                .replace("{{link}}", quizDetails[0].questions[questionNumber].deeplink)
                .replace("{{user_choice}}", selectedOption);
            // console.log(i, quizResult, `${config.staticCDN}question-text/en/${quizDetails[0].questions[i].questionId}.png`);
            whatsapp.sendMediaMsg(CAMPAIGN, student.phone, student._id, `${config.staticCDN}question-text/en/${quizDetails[0].questions[questionNumber].questionId}.png`, "IMAGE", quizResult);
            success++;
        }
    } catch (e) {
        fails++;
        console.error(e);
    }
}

async function getStudentPlayedQuiz(client, quizDetails) {
    const pipeline = getPipeline(quizDetails[0].quizId);
    await new Promise((resolve) => client.collection("whatsapp_daily_quiz_submits").aggregate(pipeline, { cursor: { batchSize: 50 } }).forEach((student) => {
        sendSolution(student, quizDetails);
    }, (err) => {
        console.error(err);
        resolve();
    }));
}

async function start(job) {
    // const today = moment().format("YYYY-MM-DD");
    // const client = (await getMongoClient()).db(db);
    // const quizDetails = await client.collection("whatsapp_daily_quizzes").find({ source: "8400400400", quizDate: today }).sort({ _id: 1 }).limit(1)
    //     .toArray();
    // await getStudentPlayedQuiz(client, quizDetails);
    // await job.progress(100);
    const cronName = "7pm-dailySolution";
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
    cron: "03 21 * * *",
};
