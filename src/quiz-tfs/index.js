/* eslint-disable guard-for-in */
const { shuffle, indexOf } = require("lodash");
const _ = require("lodash");
const { v4: uuidv4 } = require("uuid");
const { getMongoClient, redis, redshift } = require("../../modules");
const Maps = require("./map");
// a: questions
// b: chapter_alias_all_lang
// c: text_solutions
// d: mapping table
function handleOptions(string) {
    return string.replace(/'/g, "").replace(/"/g, "\\\"").trim().toUpperCase();
}
const numberMap = {
    opt_1: "1",
    opt_2: "2",
    opt_3: "3",
    opt_4: "4",
};
const defaultExpiry = 60;
const stringMap = {
    opt_1: "A",
    opt_2: "B",
    opt_3: "C",
    opt_4: "D",
};
function quotesEscape(string) {
    return string.replace(/'/g, "\\'").replace(/"/g, "\\\"").trim();
}
function optionParsing(result) {
    if (result.answer.trim().includes("::")) {
        result.type = "Multi";
    }
    if (/\d/g.test(result.answer.trim())) {
        // contain numberic
        result.optionA = { key: numberMap.opt_1, value: handleOptions(result.opt_1) };
        result.optionB = { key: numberMap.opt_2, value: handleOptions(result.opt_2) };
        result.optionC = { key: numberMap.opt_3, value: handleOptions(result.opt_3) };
        result.optionD = { key: numberMap.opt_4, value: handleOptions(result.opt_4) };
    } else {
        result.optionA = { key: stringMap.opt_1, value: handleOptions(result.opt_1) };
        result.optionB = { key: stringMap.opt_2, value: handleOptions(result.opt_2) };
        result.optionC = { key: stringMap.opt_3, value: handleOptions(result.opt_3) };
        result.optionD = { key: stringMap.opt_4, value: handleOptions(result.opt_4) };
    }
    return result;
}

async function getAvailableClassesAndSubjectsForQuiz() {
    const sql = "select a.class, a.subject, d.package_language from classzoo1.questions a left join classzoo1.chapter_alias_all_lang b on a.class = b.class and a.chapter = b.chapter left join classzoo1.text_solutions c on a.question_id = c.question_id left join classzoo1.studentid_package_mapping_new d on a.student_id = d.student_id where a.student_id < 100 and a.student_id not in (-248,-228,-223,-140,-64,-63,-41,-26,83,85,86,96) and (a.is_answered = 1 or a.is_text_answered = 1) and a.chapter is not null and b.id is not null and c.id is not null and c.opt_1 <> '' and c.answer is not null group by a.class, a.subject, d.package_language";

    return redshift.query(sql);
}

async function getChapterAndQuestions(studentClass, subject) {
    const sql = `select d.chapter_alias as chapter, c.package_language, b.question_id from classzoo1.questions as a left join classzoo1.text_solutions b on a.question_id = b.question_id left join classzoo1.studentid_package_mapping_new c on a.student_id = c.student_id left join classzoo1.chapter_alias_all_lang d on a.class=d.class and a.chapter=d.chapter where (a.is_answered = 1 or a.is_text_answered = 1) and a.class='${studentClass}' and a.subject='${subject}' and a.student_id < 100 and a.student_id not in (-248,-228,-223,-140,-64,-63,-41,-26,83,85,86,96) and a.chapter is not null and b.id is not null and b.opt_1 is not null and b.opt_1 <> '' and b.opt_2 is not null and b.opt_3 is not null and b.opt_4 is not null and b.answer is not null and d.chapter_alias is not null group by d.chapter_alias, c.package_language, b.question_id, b.created_at order by b.created_at`;
    return redshift.query(sql);
}
// add time alloted and in quiz tfs condition after we get it from the tables
async function getQuestionDetails(ids) {
    const sql = `select a.question_id, a.opt_1, a.opt_2, a.opt_3, a.opt_4, a.answer, b.question, c.student_id as mappingStudentId, c.package as mappingPackage, c.package_language as mappingPackageLanguage, c.video_language as mappingVideoLanguage, c.target_group as mappingTargetGroup, c.content_format as mappingContentFormat, c.vendor_id as mappingVendorId, c.target_group_type as mappingTargetGroupType , c.quiz_tfs_duration as questionDuration  from classzoo1.text_solutions as a left join classzoo1.questions as b on a.question_id = b.question_id left join classzoo1.studentid_package_mapping_new c on b.student_id = c.student_id where a.question_id IN (${ids.join(",")})`;
    return redshift.query(sql);
}
function getMappedSubject(subjectval, classval) {
    const subjectSplit = subjectval.split(" ");
    const key = [classval, ...subjectSplit];
    const key1 = key.join("_");
    const lookup = Maps[key1];
    let arr = lookup.split("_");
    arr = arr.slice(1, arr.length);
    const subjectnew = arr.join(" ");
    return subjectnew;
}
function getQuestionFlagAndAnswer(correctAnswer) {
    const acceptable = new Set(["A", "B", "C", "D"]);
    const acceptableInt = new Set(["1", "2", "3", "4"]);

    // const possibleStart = new Set(['(','`']);
    const returnAnswer = [];
    let flag = 1;
    let checkanswer;
    checkanswer = correctAnswer.indexOf("::") === -1 ? correctAnswer.split("::") : correctAnswer.split(",");
    checkanswer.forEach((item) => {
        item = item.trim();
        // if (possibleStart.has(item[0])) {
        //     item = item.substring(1,2);
        // }
        const checkItem = item.toUpperCase();
        returnAnswer.push(checkItem);
        if (!acceptable.has(checkItem)) flag = 0;
    });
    const returnString = returnAnswer.join(",");
    return [flag, returnString];
}

async function start(job) {
    try {
        job.progress(0);
        const mongo = (await getMongoClient()).db("doubtnut");
        redis.del("leaderboard:tests:quiztfs");
        let result = await getAvailableClassesAndSubjectsForQuiz();
        console.log(result);
        // const istOffset = moment.duration({ hours: 5, minutes: 30 });
        const startTime = new Date();

        const excludedSubjectForClass11and12 = ["ECONOMICS", "ACCOUNTS", "GENERAL KNOWLEDGE", "GENERAL KNOWLEDGE AND APTITUDE"];
        result = result.filter((item) => {
            if (+item.class === 11 || +item.class === 12) {
                return !excludedSubjectForClass11and12.includes(item.subject);
            }
            return true;
        });
        result.forEach((item) => {
            item.class = item.class.trim();
            item.subject = item.subject.trim();
        });
        await mongo.collection("QuizTfs").updateMany(
            { isActive: 1 },
            { $set: { isActive: 0 } },
        );
        const classesMapping = _.groupBy(result, "class");
        console.log(classesMapping);
        let j = 0;

        for (const classKey in classesMapping) {
            console.log(classKey);
            if (classesMapping[classKey]) {
                const subjectsMapping = _.groupBy(classesMapping[classKey], "subject");
                for (const subjectKey in subjectsMapping) {
                    if (subjectsMapping[subjectKey]) {
                        const subjectMapped = getMappedSubject(subjectKey, classKey);
                        console.log(subjectMapped);
                        // eslint-disable-next-line no-await-in-loop
                        const chapterResult = await getChapterAndQuestions(classKey, subjectKey);
                        console.log(chapterResult.length, "Ques");
                        const groupedIds = _.groupBy(chapterResult, "question_id");
                        const testIds = Object.keys(groupedIds);
                        // eslint-disable-next-line no-await-in-loop
                        const testDetailsResult = await getQuestionDetails(testIds);

                        const testDetailsById = _.groupBy(testDetailsResult, "question_id");
                        chapterResult.forEach((item) => {
                            item.package_language = item.package_language.trim();
                        });
                        const languageMapping = _.groupBy(chapterResult, "package_language");
                        for (const languageKey in languageMapping) {
                            if (languageMapping[languageKey]) {
                                const sessionIdVAalue = uuidv4();
                                let timeval = startTime.valueOf();
                                const timeOfEnd = timeval + (60 * 60 * 24 * 1000);

                                const questionsToInsert = [];
                                redis.setAsync(`QuizTfs1:${languageKey}:${classKey}:${subjectMapped}:sessionId`, sessionIdVAalue, "EX", 60 * 60 * 24);

                                // define an end time
                                // put a while loop that goes from start time to end time, and shuffle every single time
                                while (timeval < timeOfEnd) {
                                    shuffle(languageMapping[languageKey]);
                                    const startval = timeval;
                                    for (let i = 0; i < languageMapping[languageKey].length; i++) {
                                        try {
                                            if (timeval >= timeOfEnd) break;
                                            const item = languageMapping[languageKey][i];
                                            item.chapter = item.chapter.trim();

                                            const questionData = testDetailsById[item.question_id][0];
                                            const endtime = timeval + parseInt(questionData.questionduration) * 1000 + 10000;
                                            // let endtime = timeval + (30 * 1000); // time for testing

                                            const optionParsed = optionParsing(questionData);
                                            // const questionAndFlag = getQuestionFlagAndAnswer(testDetailsById[item.question_id][0].answer)
                                            // const toInclude = 1;
                                            // // const toInclude = questionAndFlag[0];
                                            // const correctAnswerChecked = questionAndFlag[1];
                                            questionData.questionText = quotesEscape(questionData.question);
                                            const doc = {
                                                questionID: item.question_id,
                                                subject: subjectMapped,
                                                language: languageKey,
                                                class: classKey,
                                                questionText: questionData.questionText,
                                                startTime: timeval,
                                                endTime: endtime,
                                                optionA: optionParsed.optionA.value,
                                                optionB: optionParsed.optionB.value,
                                                optionC: optionParsed.optionC.value,
                                                optionD: optionParsed.optionD.value,
                                                correctOption: optionParsed.answer.trim(),
                                                mappingStudentId: questionData.mappingstudentid,
                                                mappingPackage: questionData.mappingpackage,
                                                mappingPackageLanguage: questionData.mappingpackagelanguage,
                                                mappingVideoLanguage: questionData.mappingvideolanguage,
                                                mappingTargetGroup: questionData.mappingtargetgroup,
                                                mappingContentFormat: questionData.mappingcontentformat,
                                                mappingVendorId: questionData.mappingvendorid,
                                                mappingTargetGroupType: questionData.mappingtargetgrouptype,
                                                isActive: 1,
                                                sessionId: sessionIdVAalue,
                                            };
                                            timeval = endtime;
                                            questionsToInsert.push(doc);
                                        } catch (error) {
                                            console.log(testDetailsById[languageMapping[languageKey][i].question_id][0]);
                                            console.log(error);
                                        }
                                    }
                                    if (startval === timeval) break;
                                }
                                if (questionsToInsert.length) mongo.collection("QuizTfs").insertMany(questionsToInsert);
                            }
                        }
                    }
                }
            }
            job.progress(parseInt(((j + 1) / result.length) * 100));
            j++;
        }
        job.progress(100);
        return true;
    } catch (e) {
        console.log(e);
        return { e };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 0 * * *", // * Every day at 5.30 AM IST
};
