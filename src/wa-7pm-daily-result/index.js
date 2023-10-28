/* eslint-disable no-await-in-loop */
const moment = require("moment");
const { whatsapp, mysql, gupshup } = require("../../modules");
const { getMongoClient, config, redis } = require("../../modules");

const CAMPAIGN = "WA_10PM_RESULT";
const CAMPAIGN_REWARD = `${CAMPAIGN}_REWARD`;
const db = "whatsappdb";
const threeMonthsExpiry = 3 * 60 * 60 * 24 * 30;

const USER_RESULT_MSG = "<strong>❗❕RESULT TIME❕❗</strong>\n👋Hello Student {{today}} ko aayojit Daily Quiz Contest ka result\n\n✅Marks Scored :{{score}}✏️\n✅Question Attempted :{{questions_attempted}}📚\n✅Question Unattempted :{{skip_count}}❓\n✅Correct Answered :{{correct_count}}🤩\n✅Incorrect Answered :{{incorrect_count}}😅\n\n⚡ <strong>Aapki rank :{{rank}}</strong>";
let success = 0; let fails = 0;

const LEADERBOARD = "📢📢Hello bacho ,\nAaj ke ({{today}}) Daily Quiz Contest ke vijeta ye rahe\n\n1️⃣.<strong>{{user_0}}</strong>🥳\nMarks Scored-{{score_0}}\n\n2️⃣.<strong>{{user_1}}</strong>🎊\nMarks Scored-{{score_1}}\n\n3️⃣.<strong>{{user_2}}</strong>🎉\nMarks Scored-{{score_2}}\n\nTeeno vijetaon ko meri taraf se bahut bahut badhai.\n\nleader board👇\nhttps://app.doubtnut.com/seven-pm-quiz";

const REWARD_MSG = "🎙️<strong>Congratulation</strong>❗\n\n❕Aap aaj ke Daily Quiz Contest ke Vijeta hain.\nApna <strong>reward</strong>💵💰 paane ke liye neeche diye gaye link par click karke apna result check karein aur <strong>apna reward claim karein</strong>👇\nlink\nhttps://app.doubtnut.com/seven-pm-quiz";

const PARTICIPATION_MSG = "👩‍🎓👨‍🎓 <strong>Jo mehnat karte hai wo kabhi haar nahi maante.</strong>\n\nToh aaj hi kal ke <strong>Daily Quiz Contest</strong> ke liye register karein and uski ache se taiyari karein\n<strong>All the best</strong>👍";

const PDF_MSG = "👆iss PDF mein {{today}} ko huwe Daily Quiz contest ke <strong>sahi jawab aur video solutions hain</strong>";

// const quesSolution = "❓Question number: {{question_number}}\n✅Correct Answer: {{correct_option}}\n👉Your Answer: {{user_choice}}\n\n🎙️Video solution: \n{{link}}";
const ANSWER_KEY = "<strong>Answer key for Daily Quiz contest ({{today}})</strong>\n\n✅ <strong>Correct Answers</strong> 🤩\nQuestion number- {{correctQuestions}}\n\n❌ <strong>Incorrect Answers</strong> 😅\nQuestion number- {{incorrectQuestions}}\n\n🔍 <strong>Unattempted</strong> 🤔\nQuestion number- {{skipQuestions}}";

const SLOWER = 28.0;
const SUPPER = -6.7;

const registerMsg = {
    text: "👋hello Student\n<strong>kya aaj ka Daily Quiz Contest miss hogya</strong>❓\nAur ussi ke saath <strong>5000💵 tak cash prize</strong> jeetne ka mauka bhi!!🥲\n\nAgar haan toh koi baat nahi, <strong>Daily Quiz Contest</strong> kal bhi karwaya jaega🤩🤩 aur <strong>5000 tak ke cash prize bhi diye jayenge</strong>.\nToh kal ke liye bhi abhi register kar lein..",
    footer: "Neeche diye Register button ko click karke Register karein",
    action: {
        buttons: [
            { type: "reply", reply: { id: "1", title: "Play Quiz Contest" } },
            { type: "reply", reply: { id: "2", title: "Home" } },
        ],
    },
    replyType: "BUTTONS",
};

function setDailyLeaderboard(date, studentId, points) {
    redis.multi()
        .zadd(`seven_pm_quiz_leaderboard:${date}`, points, studentId)
        .expire(`seven_pm_quiz_leaderboard:${date}`, threeMonthsExpiry)
        .exec();
}

function setWeeklyLeaderboard(weekNumber, studentId, points) {
    // console.log(" student: ", studentId, " rank: ", points);
    redis.multi()
        .zadd(`seven_pm_quiz_weekly_leaderboard:${weekNumber}`, points, studentId)
        .expire(`seven_pm_quiz_weekly_leaderboard:${weekNumber}`, threeMonthsExpiry)
        .exec();
}

async function sendRegisterButton(student) {
    try {
        // console.log("##studentNotPlayedquiz: ", student.phone);
        await whatsapp.sendTextMsg(CAMPAIGN, student.phone, 0, registerMsg.text, null, null, registerMsg.footer, registerMsg.replyType, registerMsg.action);
    } catch (e) {
        console.error(e);
    }
}

// todo to be sent to those who have 2 way chatwindow open and did not play today's quiz
async function getStudentNotPlayedQuiz(client, quizArr) {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const pipeline = [{
        $match: {
            updatedAt: { $gt: yesterday },
        },
    }, {
        $project: {
            _id: 0,
            phone: 1,
        },
    }, {
        $lookup: {
            from: "whatsapp_daily_quiz_submits",
            let: { phone: "$phone" },
            pipeline: [{
                $match: {
                    $expr: {
                        $and: [{
                            $in: ["$quizId", quizArr],
                        }, {
                            $eq: ["$phone", "$$phone"],
                        }],
                    },
                },
            }, {
                $limit: 1,
            }],
            as: "sessionData",
        },
    }, {
        $match: {
            sessionData: { $size: 0 },
        },
    }, {
        $project: {
            phone: 1,
        },
    }];

    await new Promise((resolve) => client.collection("whatsapp_sessions").aggregate(pipeline, { cursor: { batchSize: 50 } }).forEach((student) => {
        sendRegisterButton(student);
    }, (err) => {
        console.error(err);
        resolve();
    }));
}

async function sendResult(student, rank, quizPdfObj, leaderboard) {
    const today = moment().format("Do MMM YY");
    try {
        const userResult = USER_RESULT_MSG.replace("{{score}}", student.totalMarks).replace("{{questions_attempted}}", student.incorrectCount + student.correctCount).replace("{{skip_count}}", 30 - (student.incorrectCount + student.correctCount))
            .replace("{{correct_count}}", student.correctCount)
            .replace("{{incorrect_count}}", student.incorrectCount)
            .replace("{{rank}}", rank)
            .replace("{{today}}", today);

        student.correctQuestions.sort((a, b) => a - b);
        student.correctQuestions = student.correctQuestions.map((x) => x + 1).join();
        student.incorrectQuestions.sort((a, b) => a - b);
        student.incorrectQuestions = student.incorrectQuestions.map((x) => x + 1).join();
        student.skipQuestions.sort((a, b) => a - b);
        student.skipQuestions = student.skipQuestions.map((x) => x + 1).join();
        const answerKey = ANSWER_KEY
            .replace("{{correctQuestions}}", student.correctQuestions)
            .replace("{{incorrectQuestions}}", student.incorrectQuestions)
            .replace("{{skipQuestions}}", student.skipQuestions)
            .replace("{{today}}", today);

        whatsapp.sendTextMsg(CAMPAIGN, student.phone, student._id, userResult);
        whatsapp.sendTextMsg(CAMPAIGN, student.phone, student._id, leaderboard);
        whatsapp.sendTextMsg(CAMPAIGN, student.phone, student._id, answerKey);

        student.questionData.sort((a, b) => a.questionNumber - b.questionNumber);
        const pdfMsg = PDF_MSG.replace("{{today}}", today);
        if(quizPdfObj[student.quizId] && quizPdfObj[student.quizId][student.locale]){
            whatsapp.sendMediaMsg(CAMPAIGN, student.phone, student._id, `${config.staticCDN}${quizPdfObj[student.quizId][student.locale]}`, "document", pdfMsg, registerMsg.replyType, registerMsg.action);
        }
        if (rank < 4) {
            whatsapp.sendTextMsg(CAMPAIGN_REWARD, student.phone, student._id, REWARD_MSG, null, null, null, registerMsg.replyType, registerMsg.action);
        } else {
            whatsapp.sendTextMsg(CAMPAIGN, student.phone, student._id, PARTICIPATION_MSG, null, null, null, registerMsg.replyType, registerMsg.action);
        }
        success++;
    } catch (e) {
        fails++;
        console.error(e);
    }
}

function getPipeline(quizIds, multiplyFactorObj, additionFactorObj, leaderboard) {
    console.log("##pipeline: quizIds: ",quizIds, "\nmultiplyFactorObj: ", multiplyFactorObj, "\n additionFactorObj:", additionFactorObj);
    const branchArr = [];
    quizIds.map((x) => { 
        branchArr.push({
        case: { $and: [ { $eq: ["$quizId", x ] }, { $eq: ["$locale", "en"] } ] }, then: { $sum: [ ((additionFactorObj[x] && additionFactorObj[x].en)? additionFactorObj[x].en : 0), { $multiply: [  (multiplyFactorObj[x]? multiplyFactorObj[x].en : 1), "$totalMarks" ] }] }
        },{
        case: { $and: [ { $eq: ["$quizId", x ] }, { $eq: ["$locale", "hi"] } ] }, then: { $sum: [ ((additionFactorObj[x] && additionFactorObj[x].hi)? additionFactorObj[x].hi : 0), { $multiply: [  (multiplyFactorObj[x]? multiplyFactorObj[x].hi : 1), "$totalMarks" ] }] }
        });
    }
    );
    // console.log("##branchArr: ", JSON.stringify(branchArr, null, 2));
    const pipeline = [{
        $match: {
            quizId: { $in: quizIds },
        },
    }, {
        $group: {
            _id: { quizId: "$quizId", studentId: "$studentId", questionNumber: "$questionNumber" },
            locale: { $first: "$locale" },
            selectedOption: { $first: "$selectedOption" },
            marks: { $first: "$marks" },
            lastSubmit: { $last: "$createdAt" },
            phone: { $first: "$phone" },
        },
    }, {
        $group: {
            _id: "$_id.studentId",
            phone: { $first: "$phone" },
            locale: { $first: "$locale"},
            quizId: { $first: "$_id.quizId"},
            questionData: {
                $push: {
                    questionNumber: "$_id.questionNumber",
                    selectedOption: "$selectedOption",
                },
            },
            lastSubmit: { $last: "$lastSubmit" },
            totalMarks: { $sum: "$marks" },
            incorrectQuestions: {
                $push: {
                    $cond: [
                        { $eq: ["$marks", -1] },
                        "$_id.questionNumber",
                        "$$REMOVE",
                    ],
                },
            },
            correctQuestions: {
                $push: {
                    $cond: [
                        { $eq: ["$marks", 4] },
                        "$_id.questionNumber",
                        "$$REMOVE",
                    ],
                },
            },
            skipQuestions: {
                $push: {
                    $cond: [
                        { $eq: ["$marks", 0] },
                        "$_id.questionNumber",
                        "$$REMOVE",
                    ],
                },
            },
            incorrectCount: {
                $sum: {
                    $cond: [
                        { $eq: ["$marks", -1] },
                        1,
                        0,
                    ],
                },
            },
            correctCount: {
                $sum: {
                    $cond: [
                        { $eq: ["$marks", 4] },
                        1,
                        0,
                    ],
                },
            },
            skipCount: {
                $sum: {
                    $cond: [
                        { $eq: ["$marks", 0] },
                        1,
                        0,
                    ],
                },
            },
        },
    }, {
        $addFields: { 
            totalMarksWithNormalization: { 
                $switch: { 
                    branches: branchArr,
                    default: "$totalMarks",
                }
            },
        },
    }, {
        $sort: { totalMarksWithNormalization: -1, incorrectCount: 1, lastSubmit: 1 },
    }];

    //console.log("##pipeline: ", JSON.stringify(pipeline[3].$project.totalMarksWithNormalization.$switch.branches, null, 2));
    if (!leaderboard) {
        pipeline.push({ $limit: 3 });
    }
    return pipeline;
}

async function getStudentPlayedQuiz(client, quizArr, multiplyFactorObj, additionFactorObj, quizPdfObj, leaderboard) {
    const pipeline = getPipeline(quizArr, multiplyFactorObj, additionFactorObj, leaderboard);
    if (!leaderboard) {
        return client.collection("whatsapp_daily_quiz_submits").aggregate(pipeline).toArray();
    }
    let rank = 0;
    const today = moment().format("YYYY-MM-DD");
    await new Promise((resolve) => client.collection("whatsapp_daily_quiz_submits").aggregate(pipeline, { cursor: { batchSize: 50 } }).forEach((student) => {
        ++rank;
        setDailyLeaderboard(today, student._id, rank);
        sendResult(student, rank, quizPdfObj, leaderboard);
    }, (err) => {
        console.error(err);
        resolve();
    }));
}
async function getNames(studentIds) {
    try {
        const sql = "select student_id, student_fname from classzoo1.students where student_id IN (?)";
        return mysql.pool.query(sql, [studentIds]).then(([res]) => res);
    } catch (err) {
        console.error(err);
    }
}
async function getLeaderBoard(client, quizArr, multiplyFactorObj, additionFactorObj, quizPdfObj) {
    const today = moment().format("Do MMM YY");
    const top3 = await getStudentPlayedQuiz(client, quizArr, multiplyFactorObj, additionFactorObj, quizPdfObj);
    console.log("##top3: ", top3);
    const winnerNames = await getNames(top3.map((x) => x._id));
    let leaderboard = LEADERBOARD.replace("{{today}}", today);
    for (let i = 0; i < 3; i++) {
        if (!top3[i]) {
            leaderboard = leaderboard.replace(`{{user_${i}}}`, "Student")
                .replace(`{{score_${i}}}`, top3[i] || "N/A");
            continue;
        }
        const studentData = winnerNames.find((x) => +x.student_id === +top3[i]._id);
        leaderboard = leaderboard.replace(`{{user_${i}}}`, (studentData && studentData.student_fname) ? studentData.student_fname : "Student")
            .replace(`{{score_${i}}}`, top3[i].totalMarks || "N/A");
    }
    return leaderboard;
}

function getListOfDatesThisWeek() {
    let currentDate = moment().startOf("isoWeek");
    const weekNumber = moment().isoWeek();
    const dates = [];
    while (dates.length < 7) {
        if (weekNumber === currentDate.isoWeek()) {
            dates.push(currentDate.format("YYYY-MM-DD"));
        }
        currentDate = currentDate.add(1, "days");
    }
    return dates;
}

async function generateWeeklyLeaderboard(client) {
    const weekNumber = moment().isoWeek();
    const weekDates = getListOfDatesThisWeek();
    const quizDetails = await client.collection("whatsapp_daily_quizzes").aggregate([{
        $match: {
            source: "8400400400",
            quizDate: { $in: weekDates },
        },
    }, {
        $group: {
            _id: { quizId: "$quizId" },
            questionData: { $first: "$questionData" }
        }
    }, {
        $project: {
            quizId: "$_id.quizId",
            enFactors: {
                $filter: {
                    input: "$questionData",
                    as: "enFactor",
                    cond: { $eq: ["$$enFactor.locale", "en"]}
                }
            },
            hiFactors: {
                $filter: {
                    input: "$questionData",
                    as: "hiFactor",
                    cond: { $eq: ["$$hiFactor.locale", "hi"]}
                }
            },
        }
    }, {
        $project: {
            quizId: 1,
            multiplyFactor: { en: { $arrayElemAt: ["$enFactors.mFactor", 0] }, hi: { $arrayElemAt: ["$hiFactors.mFactor", 0] } },
            additionFactor: { en: { $arrayElemAt: ["$enFactors.aFactor", 0] }, hi: { $arrayElemAt: ["$hiFactors.aFactor", 0] } },
        }
    }]).toArray();
    // console.log("##weekNumber: ", weekNumber, " weekDates: ", weekDates, " quizDetails: ", quizDetails);
    const quizIds = quizDetails.map((x) => x.quizId);
    let multiplyFactorObj = {};
    let additionFactorObj = {};
    for (let i = 0; i < quizDetails.length; i++ ){
        multiplyFactorObj[quizDetails[i].quizId] = quizDetails[i].multiplyFactor ;
        additionFactorObj[quizDetails[i].quizId] = quizDetails[i].additionFactor ;
    }
    const branchArr = [];
    quizIds.map((x) => { 
        branchArr.push({
        case: { $and: [ { $eq: ["$quizId", x ] }, { $eq: ["$locale", "en"] } ] }, then: { $sum: [ ((additionFactorObj[x] && additionFactorObj[x].en)? additionFactorObj[x].en : 0), { $multiply: [  ((multiplyFactorObj[x] && multiplyFactorObj[x].en) ? multiplyFactorObj[x].en : 1), "$totalMarksPerQuiz" ] }] }
        },{
        case: { $and: [ { $eq: ["$quizId", x ] }, { $eq: ["$locale", "hi"] } ] }, then: { $sum: [ ((additionFactorObj[x] && additionFactorObj[x].hi)? additionFactorObj[x].hi : 0), { $multiply: [  ((multiplyFactorObj[x] && multiplyFactorObj[x].hi) ? multiplyFactorObj[x].hi : 1), "$totalMarksPerQuiz" ] }] }
        });
    });
    await redis.del(`seven_pm_quiz_weekly_leaderboard:${weekNumber}`);
    const pipeline = [{
        $match: {
            quizId: { $in: quizIds },
        },
    }, {
        $group: {
            _id: { quizId: "$quizId", studentId: "$studentId", questionNumber: "$questionNumber" },
            locale: { $first: "$locale" },
            marks: { $first: "$marks" },
            lastSubmit: { $last: "$createdAt" },
        },
    }, {
        $group: {
            _id: { quizId: "$_id.quizId", studentId: "$_id.studentId" },
            quizId: { $first: "$_id.quizId" },
            locale: { $first: "$locale" },
            totalMarksPerQuiz: { $sum: "$marks" },
            lastSubmit: { $last: "$lastSubmit" },
            incorrectCountPerQuiz: {
                $sum: {
                    $cond: [
                        { $eq: ["$marks", -1] },
                        1,
                        0,
                    ],
                },
            },
        },
    }, {
        $addFields: { 
            totalMarksPerQuizWithNormalization: { 
                $switch: { 
                    branches: branchArr,
                    default: "$totalMarksPerQuiz",
                }
            },
        },
    }, {
        $group: {
            _id: "$_id.studentId",
            lastSubmit: { $last: "$lastSubmit" },
            totalMarks: { $sum: "$totalMarksPerQuiz"},
            totalMarksWithNormalization: { $sum: "$totalMarksPerQuizWithNormalization" },
            incorrectCount: { $sum: "$incorrectCountPerQuiz" }
        },
    }, {
        $sort: { totalMarksWithNormalization: -1, incorrectCount: 1, lastSubmit: 1 },
    }];
    let rank = 0;
    await new Promise((resolve) => client.collection("whatsapp_daily_quiz_submits").aggregate(pipeline, { allowDiskUse: true }, { cursor: { batchSize: 50 } }).forEach((student) => {
        ++rank;
        setWeeklyLeaderboard(weekNumber, student._id, rank);
    }, (err) => {
        console.error(err);
        resolve();
    }));
}
async function getTupper(client, quizIdToday, locale){
    try{ 
        // const totalDocs = await client.collection("whatsapp_daily_quiz_submits").distinct("phone", { quizId: quizIdToday, locale: locale });
        // let top1Percent = Math.ceil(totalDocs.length * 0.01);
        // console.log("totalDocs: ", totalDocs, " top1percent: ", top1Percent);
        // top1Percent = Math.max(top1Percent, 10);
        // console.log("##top1Percent: ", top1Percent, " locale: ", locale, " quizIdToday: ", quizIdToday);
        // const pipeline = [{
        //     $match: {
        //         quizId: quizIdToday,
        //         locale,
        //     },
        // }, {
        //     $group: {
        //         _id: { quizId: "$quizId", studentId: "$studentId", questionNumber: "$questionNumber" },
        //         marks: { $first: "$marks" },
        //     },
        // }, {
        //     $group: {
        //         _id: "$_id.studentId",
        //         totalMarks: { $sum: "$marks" },
        //     },
        // }, {
        //     $sort: { totalMarks: -1 },
        // }, {
        //     $limit: top1Percent
        // }, {
        //     $group: {
        //         _id: null,
        //         topAverage: { $avg: "$totalMarks" } 
        //     }
        // }];
        // const tupper = await client.collection("whatsapp_daily_quiz_submits").aggregate(pipeline).toArray();
        // return (tupper[0]) ? tupper[0].topAverage : SUPPER;

        const pipeline = [{
            $match: {
                quizId: quizIdToday,
                locale,
            },
        }, {
            $group: {
                _id: { quizId: "$quizId", studentId: "$studentId", questionNumber: "$questionNumber" },
                marks: { $first: "$marks" },
            },
        }, {
            $group: {
                _id: "$_id.studentId",
                totalMarks: { $sum: "$marks" },
            },
        }, {
            $sort: { totalMarks: -1 },
        }, {
            $group: {
                _id: null,
                stdDev: { $stdDevPop: "$totalMarks" },
                mean: { $avg: "$totalMarks" }
            }
        }, {
            $addFields: {
                tupper: { $subtract: [ "$mean", "$stdDev" ] }
            }
        }];
        const value = await client.collection("whatsapp_daily_quiz_submits").aggregate(pipeline).toArray();
        // console.log("##value: ", value);
        return !value[0] || value[0].tupper === 0 ? SLOWER: value[0].tupper;
    
    } catch (e) {
        console.error(e);
    }
}

async function getTlower(client, quizIdToday, locale){
    const pipeline = [{
        $match: {
            quizId: quizIdToday,
            locale,
        },
    }, {
        $group: {
            _id: { quizId: "$quizId", studentId: "$studentId", questionNumber: "$questionNumber" },
            marks: { $first: "$marks" },
        },
    }, {
        $group: {
            _id: "$_id.studentId",
            totalMarks: { $sum: "$marks" },
        },
    }, {
        $sort: { totalMarks: -1 },
    }, {
        $group: {
            _id: null,
            stdDev: { $stdDevPop: "$totalMarks" },
            mean: { $avg: "$totalMarks" }
        }
    }, {
        $addFields: {
            tlower: { $sum: [ "$stdDev", "$mean" ] }
        }
    }];
    const value = await client.collection("whatsapp_daily_quiz_submits").aggregate(pipeline).toArray();
    // console.log("##value: ", value);
    return !value[0] || value[0].tlower === 0 ? SLOWER: value[0].tlower;
}

async function start(job) {
    const today = moment().format("YYYY-MM-DD");

    const client = (await getMongoClient()).db(db);
    const quizDetails = await client.collection("whatsapp_daily_quizzes").find({ source: "8400400400", quizDate: today }, { quizId: 1, batchId: 1 } ).sort({ batchId: 1 }).toArray();
    const quizArr = quizDetails.map((x) => (x.quizId));
    //const quizArr = ["ee207b31-536c-4038-a46f-4f4ddd34bfdc", "ede751a6-4252-450e-a174-0ce12de4aa95", "c4c3b24a-d9a3-4f79-8f9a-0aae746f5863", "fb6c8156-0d0b-470c-8008-a8f57c57fae1", "99e5d9b8-6336-421d-aa8d-3693aa5b93b9", "a5643a77-7192-4751-b498-19055bd27c7c", "731fbe53-8536-4f10-b267-4e45bdfb6906"];
    console.log("##quizArr: ", quizArr);
    const slower = SLOWER;
    const supper = SUPPER;
    let multiplyFactorObj = {};
    let additionFactorObj = {};
    for (let i = 0; i < quizArr.length; i++){
        let tvalues = await Promise.all([getTupper(client, quizArr[i], "en"), getTupper(client, quizArr[i], "hi"), getTlower(client, quizArr[i], "en"), getTlower(client, quizArr[i], "hi")]);
        let tupper = [ tvalues[0], tvalues[1]];
        let tlower = [ tvalues[2], tvalues[3]];
        //console.log("##tlower: ", tlower, "\n##tupper: ", tupper, "\n##tvalues: ", tvalues);
        tlower = tlower.map((x) => +x.toFixed(2));
        tupper = tupper.map((x) => +x.toFixed(2));
        //console.log("supper: ", supper, "\nslower: ", slower, "\ntlower: ", tlower,"\ntupper: ",tupper);
        
        const multiplyFactor = { 
            "en": (supper - slower)/(tupper[0] - tlower[0]), 
            "hi": (supper - slower)/(tupper[1] - tlower[1])
        };
        const additionFactor = {
            "en": ((slower*tupper[0]) - (supper*tlower[0]))/(tupper[0] - tlower[0]),
            "hi": ((slower*tupper[1]) - (supper*tlower[1]))/(tupper[1] - tlower[1])
        };
        if((tupper[0] - tlower[0]) === 0) {
            multiplyFactor.en = 1;
            additionFactor.en = 0;
        }
        if((tupper[1] - tlower[1]) === 0) {
            multiplyFactor.hi = 1;
            additionFactor.hi = 0;
        }
        multiplyFactorObj[quizArr[i]] = multiplyFactor;
        additionFactorObj[quizArr[i]] = additionFactor;
        await client.collection("whatsapp_daily_quizzes").updateOne({ quizId: quizArr[i], "questionData.locale": "en" }, { $set: { "questionData.$.mFactor": multiplyFactor.en, "questionData.$.aFactor": additionFactor.en }});
        await client.collection("whatsapp_daily_quizzes").updateOne({ quizId: quizArr[i], "questionData.locale": "hi" }, { $set: { "questionData.$.mFactor": multiplyFactor.hi, "questionData.$.aFactor": additionFactor.hi }});
    }
    console.log("##multiplyFactorObj: ", multiplyFactorObj, "\n##additionFactorObj:  ", additionFactorObj);
    const quizPdfArr = await client.collection("whatsapp_daily_quizzes").aggregate([{ $match: { source: "8400400400", quizDate: today } },{ $project: { quizId: "$quizId", pdfUrl: "$questionData.pdfUrl" }}]).toArray();
    let quizPdfObj = {};
    for (let i = 0;i < quizPdfArr.length; i++ ){
        if(quizPdfArr[i].pdfUrl){
            const pdfEn = quizPdfArr[i].pdfUrl.find(pdf => pdf.endsWith("en.pdf"));
            const pdfHi = quizPdfArr[i].pdfUrl.find(pdf => pdf.endsWith("hi.pdf"));
            quizPdfObj[quizPdfArr[i].quizId] = { "en": pdfEn, "hi": pdfHi };
        }
    }
    console.log("##quizPdfObj: ", quizPdfObj);
    success = 0; fails = 0;
    const leaderboard = await getLeaderBoard(client, quizArr, multiplyFactorObj, additionFactorObj, quizPdfObj);
    console.log("##leaderboard: ", leaderboard);
    await job.progress(10);
    await getStudentPlayedQuiz(client, quizArr, multiplyFactorObj, additionFactorObj, quizPdfObj, leaderboard);
    await job.progress(50);
    // await getStudentNotPlayedQuiz(client, quizArr);
    await job.progress(70);
    // await generateWeeklyLeaderboard(client);
    const cronName = "7pm-dailyResult";
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
    cron: "03 22 * * *",
};


// ["be2b2f6e-d326-4147-bfe9-e3e5772357ff", "4892d2c0-3c25-4f3d-ae29-80e1861eb144", "1c3648fa-759d-4937-89a8-30f149b0c7ec", "b1c8d675-4716-46c1-9ef5-e0b46a81f025", "0d4bac03-6925-40c8-8a25-29ae1c82148b"]
// 8400400400 to c
//         return (tlower[0].topAverage) ? tlower[0].topAverage : 1;
// miro_test - whatsappdb
// pipeline limit 3
