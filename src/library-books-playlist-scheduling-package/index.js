/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const redisClient = require("../../modules/redis");
const {
    mysql, config,
} = require("../../modules");

function getPackageData() {
    const sql = "SELECT student_id,class,subject  FROM studentid_package_details WHERE package_type in ('books','ncert', 'coaching') and class=6 and is_active > 0";
    console.log(sql);
    return mysql.pool.query(sql).then((res) => res[0]);
}

function getChapterListData(studentId, studentClass, subject) {
    const sql = `SELECT chapter, question_id FROM questions where student_id=${studentId} and class=${studentClass} and subject='${subject}' and (is_answered=1 or is_text_answered=1) GROUP by chapter`;
    // console.log(sql);
    return mysql.pool.query(sql).then((res) => res[0]);
}

function updateStudentPackagedetails(studentId, studentClass, subject) {
    const sql = `UPDATE studentid_package_details SET is_active=0 where student_id=${studentId} and class=${studentClass} and subject='${subject}'`;
    // console.log(sql);
    return mysql.writePool.query(sql).then((res) => res[0]);
}

function getExerciseListData(studentId, studentClass, subject, chapter) {
    const sql = `SELECT b.id as text_solutions_id, b.type, a.question_id FROM (SELECT * from questions where student_id=${studentId} and class=${studentClass} and subject='${subject}' and chapter LIKE '${chapter}' and (is_answered=1 or is_text_answered=1)) as a inner join text_solutions as b on a.question_id = b.question_id GROUP by b.type order by b.id`;
    // console.log(sql);
    return mysql.pool.query(sql).then((res) => res[0]);
}

function getPDFData(studentId, studentClass, subject, chapter) {
    const sql = `SELECT pdf_url from chapter_pdf_details where student_id=${studentId} and class=${studentClass} and subject='${subject}' and chapter LIKE '${chapter}' and is_pdf_ready=1`;
    // console.log(sql);
    return mysql.pool.query(sql).then((res) => res[0]);
}

function getQuestionList(studentId, studentClass, subject, chapter, type) {
    const sql = `SELECT a.question_id FROM (SELECT * from questions where student_id=${studentId} and class=${studentClass} and subject='${subject}' and chapter LIKE '${chapter}' and (is_answered=1 or is_text_answered=1)) as a inner join text_solutions as b on a.question_id = b.question_id and b.type='${type}' GROUP by b.question_id order by a.question_id`;
    // console.log(sql);
    return mysql.pool.query(sql).then((res) => res[0]);
}

function getChapterQuestionList(studentId, studentClass, subject, chapter) {
    const sql = `SELECT question_id FROM questions where student_id=${studentId} and class=${studentClass} and subject='${subject}' and chapter LIKE '${chapter}' and is_answered=1  order by question_id`;
    // console.log(sql);
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function libraryBooksDataPopulate(job) {
    const allBookData = await getPackageData();
    console.log("package length", allBookData.length);
    for (let i = 0; i < allBookData.length; i++) {
        console.log("BOOK--------", i);
        if (allBookData[i].student_id && allBookData[i].class && allBookData[i].subject) {
            const packageData = allBookData[i];
            const chapterList = await getChapterListData(packageData.student_id, packageData.class, packageData.subject);
            console.log("chapterList length", chapterList.length, packageData.student_id, packageData.class, packageData.subject);
            if (chapterList.length === 0) {
                await updateStudentPackagedetails(packageData.student_id, packageData.class, packageData.subject);
            } else {
                const chapterData = [];
                let chapterLengthFlag = 0;
                for (let j = 0; j < chapterList.length; j++) {
                    if (!chapterList[j].chapter) {
                        continue;
                    }
                    let count = 0;
                    const list = {};
                    list.id = chapterList[j].question_id;
                    list.name = _.startCase(chapterList[j].chapter);
                    list.view_type = "FLEX";
                    list.description = "";
                    list.image_url = null;
                    list.is_first = 0;
                    list.is_last = 0;
                    list.empty_text = null;
                    list.student_class = packageData.class;
                    list.resource_type = "playlist";
                    list.subject = packageData.subject;
                    list.flex_list = [];

                    chapterList[j].chapter = chapterList[j].chapter.replace(/'/g, "''").replace(/`/g, "");
                    const pdfData = await getPDFData(packageData.student_id, packageData.class, packageData.subject, chapterList[j].chapter);
                    if (pdfData && pdfData.length && pdfData[0].pdf_url) {
                        list.pdf_meta_info = { pdf_url: `${config.staticCDN}${pdfData[0].pdf_url}` };
                    }

                    const exerciseList = await getExerciseListData(packageData.student_id, packageData.class, packageData.subject, chapterList[j].chapter);
                    console.log("exerciseList length", exerciseList.length);
                    if (exerciseList.length && exerciseList[0].type && exerciseList[0].type.length) {
                        for (let k = 0; k < exerciseList.length; k++) {
                            if (exerciseList[k].type && exerciseList[k].type.length) {
                                const flexData = {};
                                flexData.id = exerciseList[k].question_id;
                                flexData.name = _.startCase(exerciseList[k].type);
                                flexData.view_type = "LIST";
                                flexData.description = "";
                                flexData.image_url = null;
                                flexData.is_first = 0;
                                flexData.is_last = 1;
                                flexData.empty_text = null;
                                flexData.student_class = packageData.class;
                                flexData.resource_type = "playlist";
                                flexData.subject = packageData.subject;
                                // flexData.package_details_id = `LIBRARY_NEW_BOOK_${packageData.student_id}_${packageData.class}_${packageData.subject}_${flexData.id}`;
                                flexData.package_details_id = `LIBRARY_NEW_BOOK_${packageData.student_id}_${packageData.class}_${packageData.subject}_${flexData.id}_${exerciseList[k].text_solutions_id}`;

                                exerciseList[k].type = exerciseList[k].type.replace(/'/g, "''").replace(/`/g, "''");
                                const questionList = await getQuestionList(packageData.student_id, packageData.class, packageData.subject, chapterList[j].chapter, exerciseList[k].type);

                                if (questionList.length) {
                                    const qList = [];
                                    questionList.forEach((x) => {
                                        qList.push(x.question_id);
                                    });
                                    console.log("redis write 1 sucess");
                                    list.flex_list.push(flexData);

                                    count += questionList.length;
                                    await redisClient.del(flexData.package_details_id);
                                    redisClient.set(flexData.package_details_id, JSON.stringify(qList), "EX", 60 * 60 * 24 * 180);
                                }
                            }
                        }
                    } else {
                        const questionChapterList = await getChapterQuestionList(packageData.student_id, packageData.class, packageData.subject, chapterList[j].chapter);
                        if (questionChapterList.length) {
                            const flexData = {};
                            flexData.id = questionChapterList[0].question_id;
                            flexData.name = "All Questions";
                            flexData.view_type = "LIST";
                            flexData.description = "";
                            flexData.image_url = null;
                            flexData.is_first = 0;
                            flexData.is_last = 1;
                            flexData.empty_text = null;
                            flexData.student_class = packageData.class;
                            flexData.resource_type = "playlist";
                            flexData.subject = packageData.subject;
                            flexData.package_details_id = `LIBRARY_NEW_BOOK_${packageData.student_id}_${packageData.class}_${packageData.subject}_${flexData.id}_${flexData.id}`;

                            if (questionChapterList.length) {
                                const qList = [];
                                questionChapterList.forEach((x) => {
                                    qList.push(x.question_id);
                                });
                                console.log("redis write 2 is success in case of no excercise");
                                list.flex_list.push(flexData);
                                count = questionChapterList.length;
                                await redisClient.del(flexData.package_details_id);
                                redisClient.set(flexData.package_details_id, JSON.stringify(qList), "EX", 60 * 60 * 24 * 180);
                            }
                        } else {
                            chapterLengthFlag += 1;
                        }
                    }

                    list.description = `#!#${count}`;
                    if (list.flex_list.length) {
                        chapterData.push(list);
                    }
                }
                if (chapterLengthFlag === chapterList.length) {
                    console.log("chapter exist but there is no solutions is_text_answered and is_answered marked wrong");
                    await updateStudentPackagedetails(packageData.student_id, packageData.class, packageData.subject);
                }
                await redisClient.del(`LIBRARY_NEW_BOOK_${packageData.student_id}_${packageData.class}_${packageData.subject}`);
                redisClient.set(`LIBRARY_NEW_BOOK_${packageData.student_id}_${packageData.class}_${packageData.subject}`, JSON.stringify(chapterData), "EX", 60 * 60 * 24 * 180);
            }
        }
        await job.progress(parseInt((i * 100) / allBookData.length));
    }
}

async function start(job) {
    await libraryBooksDataPopulate(job);
    await job.progress(100);
    console.log(`the script successfully ran at ${new Date()}`);
    return { data: "success" };
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 0/1 * * *",
};
