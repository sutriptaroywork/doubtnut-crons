const { getMongoClient, redis } = require("../../modules");

const db = "whatsappdb";
const dbSubmits = "whatsapp_daily_quiz_submits";
const threeMonthsExpiry = 3 * 60 * 60 * 24 * 30;

function setWeeklyLeaderboard(studentId, points) {
    // console.log(" student: ", studentId, " rank: ", points);
    redis.multi()
        .zadd(`seven_pm_quiz_weekly_leaderboard:45`, points, studentId)
        .expire(`seven_pm_quiz_weekly_leaderboard:45`, threeMonthsExpiry)
        .exec();
}

async function getPreviousScores(client) {
    const pipeline = [{
        $match: {
            quizId: { $in: ["8f79c0d3-0032-4e71-ac36-2fcfdf7f074f", "0b956d34-7e98-42a1-b70e-637859b1618c", "e65ddd52-b456-4b96-9afb-30cff2864c01", "e3142582-4f2c-45c6-8379-9d9fab94dbfe"] },
        }
    }, {
        $group: {
            _id: { quizId: "$quizId", studentId: "$studentId", questionNumber: "$questionNumber" },
            // selectedOption: { $first: "$selectedOption" },
            marks: { $first: "$marks" },
            lastSubmit: { $last: "$createdAt" },
        },
    }, {
        $group: {
            _id: "$_id.studentId",
            lastSubmit: { $last: "$lastSubmit" },
            totalMarks: { $sum: "$marks" },
            incorrectCount: {
                $sum: {
                    $cond: [
                        { $eq: ["$marks", -1] },
                        1,
                        0
                    ]
                },
            },
        },
    }, {
        $sort: { totalMarks: -1, incorrectCount: 1, lastSubmit: 1 },
    }];
    const oldQuizScoresArr = await client.collection(dbSubmits).aggregate(pipeline, { allowDiskUse: true }, { cursor: { batchSize: 50 } }).toArray();
    return oldQuizScoresArr;
}

async function getNewScores(client) {
    const quizDetails = await client.collection("whatsapp_daily_quizzes").aggregate([{
        $match: {
            source: "8400400400",
            quizDate: { $in: ["2021-11-12", "2021-11-13", "2021-11-14"] },
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
    const quizIds = quizDetails.map((x) => x.quizId);
    let multiplyFactorObj = {};
    let additionFactorObj = {};
    for (let i = 0; i < quizDetails.length; i++ ){
        multiplyFactorObj[quizDetails[i].quizId] = quizDetails[i].multiplyFactor ;
        additionFactorObj[quizDetails[i].quizId] = quizDetails[i].additionFactor ;
    }
    console.log("##quizIds: ", quizIds, "\nmultiplyFactorObj: ", multiplyFactorObj, "\nadditionFactorObj:", additionFactorObj);
    const branchArr = [];
    quizIds.map((x) => { 
        branchArr.push({
        case: { $and: [ { $eq: ["$quizId", x ] }, { $eq: ["$locale", "en"] } ] }, then: { $sum: [ ((additionFactorObj[x] && additionFactorObj[x].en)? additionFactorObj[x].en : 0), { $multiply: [  ((multiplyFactorObj[x] && multiplyFactorObj[x].en) ? multiplyFactorObj[x].en : 1), "$totalMarksPerQuiz" ] }] }
        },{
        case: { $and: [ { $eq: ["$quizId", x ] }, { $eq: ["$locale", "hi"] } ] }, then: { $sum: [ ((additionFactorObj[x] && additionFactorObj[x].hi)? additionFactorObj[x].hi : 0), { $multiply: [  ((multiplyFactorObj[x] && multiplyFactorObj[x].hi) ? multiplyFactorObj[x].hi : 1), "$totalMarksPerQuiz" ] }] }
        });
    });
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
    const newScoresArr = await client.collection("whatsapp_daily_quiz_submits").aggregate(pipeline, { allowDiskUse: true }).toArray();
    return newScoresArr;
}

async function start(job) {
    const client = (await getMongoClient()).db(db);
    success = 0; fails = 0;
    const previousScores = await getPreviousScores(client);
    // console.log("#previousScores: ", previousScores);
    const newScores = await getNewScores(client);
    // console.log("#newScores: ", newScores);
    await job.progress(20);
    const resultObj = {};
    newScores.forEach((x) => { resultObj[x._id]= {"_id": x._id, "lastSubmit": x.lastSubmit, "totalMarks": x.totalMarks, "incorrectCount": x.incorrectCount, "totalMarksWithNormalization": x.totalMarksWithNormalization};});
    // console.log("##resultObj before merge: ", resultObj);
    previousScores.forEach((x) => { 
        if(resultObj[x._id]){
            resultObj[x._id]["incorrectCount"] = resultObj[x._id]["incorrectCount"] ? (resultObj[x._id]["incorrectCount"] + x.incorrectCount) : x.incorrectCount;
            resultObj[x._id]["totalMarksWithNormalization"] = resultObj[x._id]["totalMarksWithNormalization"] ? (resultObj[x._id]["totalMarksWithNormalization"] + x.totalMarks) : x.totalMarks;
        } else {
            resultObj[x._id] = x;
            resultObj[x._id]["totalMarksWithNormalization"] = x.totalMarks;  
        }
    });
    // console.log("##resultObj after merge: ", Object.keys(resultObj).length);
    await redis.del(`seven_pm_quiz_weekly_leaderboard:45`);
    await job.progress(70);
    const sortedObj = Object.values(resultObj).sort(function (a, b){ 
    if(a.totalMarksWithNormalization === b.totalMarksWithNormalization){
        return a.incorrectCount - b.incorrectCount;
    }
    return b.totalMarksWithNormalization - a.totalMarksWithNormalization;});
    console.log(sortedObj);
    sortedObj.forEach((x, ind) => {
        setWeeklyLeaderboard(x._id, ind + 1);
    });
    await job.progress(100);
}
module.exports.start = start;
module.exports.opts = {
    cron: "40 09 14 11 *",
};
