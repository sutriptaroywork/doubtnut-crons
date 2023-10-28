const _ = require("lodash");
const moment = require("moment");
const { mysql } = require("../../modules");
const { redshift } = require("../../modules");

async function getAllDetailsRegisteredStudents(testId) {
    const sql = `select * from classzoo1.scholarship_test where test_id in (${testId})`;
    const users = await redshift.query(sql).then((res) => res);
    return users;
}

async function getScholarshipDetails() {
    const sql = "select * from scholarship_exam where is_active = 1";
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getTestSeriesData(studentId, testId) {
    const sql = `select * from classzoo1.testseries_student_subscriptions where student_id in (${studentId}) and test_id in (${testId})`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getTestSectionByTestSeriesId(testId) {
    const sql = `SELECT section_code,title,description,test_id FROM testseries_sections WHERE test_id in (${testId}) AND is_active = 1 ORDER BY order_pref ASC`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getResultByTestSubscriptionId(Id) {
    const sql = `SELECT * FROM classzoo1.testseries_student_results WHERE test_subscription_id = ${Id}`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getAllTestQuestionsByTestIdWithData(testId) {
    const sql = `SELECT * FROM testseries_questions INNER JOIN testseries_question_bank ON testseries_questions.questionbank_id = testseries_question_bank.id WHERE test_id in (${testId}) AND is_active = 1`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getAllOptionsByQuestionIds(questionBankKeysString) {
    const sql = `SELECT * FROM testseries_question_answers WHERE questionbank_id IN (${questionBankKeysString})`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function setScholarshipMarks(finalString, studentId, testId) {
    const sql = `update scholarship_test set subject_level_marks = '${finalString}' where student_id = ${studentId} and test_id = ${testId}`;
    console.log(sql);
    return mysql.writePool.query(sql).then((res) => res[0]);
}

async function start(job) {
    try {
        let startProcess = false;
        const scholarshipDetails = await getScholarshipDetails();
        const scholarshipArray = [];
        // check if the result has been declared
        if (scholarshipDetails && scholarshipDetails[0]) {
            for (let i = 0; i < scholarshipDetails.length; i++) {
                const scholarshipDate = moment(scholarshipDetails[i].result_time).add(8, "hours").format();
                const scholarshipDate2 = moment(scholarshipDetails[i].result_time).add(8, "hours").add(35, "minutes").format();
                if (moment().add(5, "hours").add(30, "minutes").isAfter(scholarshipDate) && moment().add(5, "hours").add(30, "minutes").isBefore(scholarshipDate2)) {
                    startProcess = true;
                    scholarshipArray.push(scholarshipDetails[i].test_id);
                }
            }
        }
        if (startProcess) {
            // get the details of all the registered students and tests sections of scholarship tests
            const studentsDetails = await getAllDetailsRegisteredStudents(scholarshipArray);
            const testSectionsData = await getTestSectionByTestSeriesId(scholarshipArray);
            const studentsDetailsArray = [];
            studentsDetails.forEach((e) => {
                studentsDetailsArray.push(e.student_id);
            });
            const subscriptionId = await getTestSeriesData(studentsDetailsArray, scholarshipArray);
            let questionsOptionDataGroupedAll;
            const questionsOptionDataGroupedArray = [];
            const testQuestionsDataAll = [];
            // get questions data for all tests
            for (let i = 0; i < scholarshipArray.length; i++) {
                // eslint-disable-next-line no-await-in-loop
                const testQuestionsDataTest = await getAllTestQuestionsByTestIdWithData(scholarshipArray[i]);
                const questionBankKeysString = _.join(_.keys(_.groupBy(testQuestionsDataTest, "questionbank_id")), ",");
                testQuestionsDataAll.push(testQuestionsDataTest);
                // eslint-disable-next-line no-await-in-loop
                const questionsOptionData = await getAllOptionsByQuestionIds(questionBankKeysString);
                questionsOptionDataGroupedAll = _.groupBy(questionsOptionData, "questionbank_id");
                questionsOptionDataGroupedArray.push(questionsOptionDataGroupedAll);
            }
            const workers = [];
            // get marks of each student for the corresponding test
            for (let i = 0; i < studentsDetailsArray.length; i++) {
                let subscriptionIdForStudent = subscriptionId.filter((e) => e.student_id == studentsDetailsArray[i]);
                if (subscriptionIdForStudent && subscriptionIdForStudent[0]) {
                    for (let j = 0; j < subscriptionIdForStudent.length; j++) {
                        if (subscriptionIdForStudent && subscriptionIdForStudent[j] && subscriptionIdForStudent[j].status === "COMPLETED") {
                            subscriptionIdForStudent = [subscriptionIdForStudent[j]];
                            break;
                        }
                    }
                }
                // check for completion of test
                if (subscriptionIdForStudent && subscriptionIdForStudent[0] && subscriptionIdForStudent[0].status === "COMPLETED") {
                    if (subscriptionIdForStudent[0].test_id != studentsDetails[i].test_id) {
                        continue;
                    }
                    const testSections = testSectionsData.filter((e) => e.test_id == studentsDetails[i].test_id);
                    const testSectionsCopy = _.cloneDeep(testSections);
                    const groupedTestSections = _.groupBy(testSectionsCopy, "section_code");
                    // eslint-disable-next-line no-await-in-loop
                    const result = await getResultByTestSubscriptionId(subscriptionIdForStudent[0].id);
                    const resultGrouped = _.groupBy(result, "questionbank_id");
                    const index = scholarshipArray.indexOf(parseInt(studentsDetails[i].test_id));
                    const testQuestionsData = testQuestionsDataAll[index];
                    const questionsOptionDataGrouped = questionsOptionDataGroupedArray[index];
                    const questionWiseResult = [];
                    _.forEach(testQuestionsData, (question) => {
                        const questionOptions = questionsOptionDataGrouped[question.questionbank_id];
                        const questionResult = resultGrouped[question.questionbank_id][0];
                        const questionResultOptions = _.split(questionResult.response_options, ",");
                        for (let j = questionOptions.length - 1; j >= 0; j--) {
                            if (question.type == "TEXT") {
                                questionOptions[j].title = questionResultOptions[0];
                                questionOptions[j].is_selected = 1;
                            } else if (_.includes(questionResultOptions, questionOptions[j].option_code)) {
                                questionOptions[j].is_selected = 1;
                            } else {
                                questionOptions[j].is_selected = 0;
                            }
                        }
                        question.is_correct = questionResult.is_correct;
                        question.marks_scored = questionResult.marks_scored;

                        question.options = questionOptions;
                        question.is_skipped = questionResult.is_skipped;
                        question.marks_scored = questionResult.marks_scored;
                        question.section_title = groupedTestSections[question.section_code][0].title;
                        questionWiseResult.push(question);
                    });
                    // group section wise marks for the student
                    const groupedformatedData = _.groupBy(questionWiseResult, "section_code");
                    const sectionMeta = _.map(testSectionsCopy, (section) => {
                        section.correct = _.sumBy(groupedformatedData[section.section_code], "is_correct");
                        section.skipped = _.sumBy(groupedformatedData[section.section_code], "is_skipped");
                        section.marks_scored = _.sumBy(groupedformatedData[section.section_code], (item) => parseInt(item.marks_scored));
                        section.incorrect = groupedformatedData[section.section_code].length - section.correct - section.skipped;
                        delete section.section_code;
                        delete section.title;
                        return section;
                    });
                    const finalString = [];
                    // make the required string with section wise marks
                    for (let j = 0; j < sectionMeta.length; j++) {
                        let total = 0;
                        const results = testQuestionsData.filter((e) => e.section_code == sectionMeta[j].description);
                        if (results && results[0]) {
                            for (let k = 0; k < results.length; k++) {
                                total += parseInt(results[k].correct_reward);
                            }
                        }
                        const str = ` ${sectionMeta[j].description}:- ${sectionMeta[j].marks_scored}/${total}`;
                        finalString.push(str);
                    }
                    const finalData = finalString.join();
                    workers.push(setScholarshipMarks(finalData, studentsDetailsArray[i], studentsDetails[i].test_id));
                }
            }
            // update the records in scholarship_test table
            await workers;
        }
        job.progress(100);
        return {
            data: {
                done: true,
            },
        };
    } catch (err) {
        console.log(err);
        return { err };
    }
}
module.exports.start = start;
module.exports.opts = {
    cron: "0 * * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
