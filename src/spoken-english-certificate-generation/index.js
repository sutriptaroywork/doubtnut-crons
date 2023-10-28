const axios = require("axios");
const { mysql, redis, config } = require("../../modules");

const BATCH_SIZE = 50;
const COURSE_NAME = "Spoken English Course";
const COURSE_ID = 69;
const HASH_EXPIRY = 60 * 60 * 24; // 1day

const getPrevDate = (day) => {
    const dt = new Date();
    dt.setDate(dt.getDate() - day);
    return dt.toISOString().slice(0, 10);
};

function getNewStudents(questionId, studentIds, date) {
    if (studentIds.length > 0) {
        const sql = "select * from survey_feedback where question_id = ? and student_id not in (?) and `time` > ?";
        return mysql.pool.query(sql, [questionId, studentIds, date]).then((res) => res[0]);
    }
    const sql = "select * from survey_feedback where question_id = ? and `time` > ?";
    return mysql.pool.query(sql, [questionId, date]).then((res) => res[0]);
}

function getPrevCertificates(courseId) {
    const sql = "SELECT cc.* FROM course_certificates cc LEFT JOIN (SELECT * from course_certificates where is_deleted = 0) cc_b ON cc.student_id = cc_b.student_id AND cc.course_id = cc_b.course_id AND cc.created_at < cc_b.created_at where cc_b.created_at is null and cc.course_id = ? GROUP by  cc.student_id, cc.course_id";
    return mysql.pool.query(sql, [courseId]).then((res) => res[0]);
}

async function generateCertificate(studentId, studentName, courseId, courseName) {
    const postData = {
        studentId,
        studentName,
        courseId,
        courseName,
    };

    // console.log(config.microUrl);
    const microConfig = {
        method: "post",
        url: `${config.microUrl}api/certificate/generate`,
        headers: {
            "Content-Type": "application/json",
            "x-auth-token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6NzI0NTE1LCJpYXQiOjE1OTY1MjIwNjgsImV4cCI6MTY1OTU5NDA2OH0.jCnoQt_VhGjC6EMq_ObPl9QpkBJNEAqQhPojLG_pz8c",
        },
        data: postData,
    };

    try {
        const { data } = await axios(microConfig);
        return data;
    } catch (e) {
        return null;
    }
}

async function processAllCertificates(students) {
    const processPromises = [];

    for (let i = 0; i < students.length; i += BATCH_SIZE) {
        const currStudents = students.slice(i, i + BATCH_SIZE);

        const generatePromises = [];

        for (let j = 0; j < currStudents.length; j++) {
            const eachStudent = currStudents[j];
            const { student_id: studentId, question_id: courseId, feedback: studentName } = eachStudent;

            generatePromises.push(generateCertificate(studentId, studentName, courseId, COURSE_NAME));
        }
        // eslint-disable-next-line no-await-in-loop
        await Promise.all(generatePromises);
        processPromises.push(...generatePromises);
    }
    return Promise.all(processPromises);
}
async function start(job) {
    try {
        job.progress(0);
        const prevDate = getPrevDate(2);

        const alreadyGeneratedCerts = await getPrevCertificates(COURSE_ID);
        const alreadyGeneratedSids = alreadyGeneratedCerts.map((eachStudent) => eachStudent.student_id);

        const newStudents = await getNewStudents(COURSE_ID, alreadyGeneratedSids, prevDate);

        const processedList = await processAllCertificates(newStudents);

        console.log("new", newStudents);
        console.log("processed", processedList);
        const newGeneratedSids = newStudents
            .filter((eachStudent, id) => {
                if (processedList[id]) {
                    return true;
                }
                return false;
            })
            .map((eachStudent) => eachStudent.student_id);

        console.log("created", newGeneratedSids);
        const todaysDate = getPrevDate(0);
        if (newGeneratedSids.length > 0) {
            redis.setAsync("SpokenEnglish:CERTIFICATES:Latest", JSON.stringify(newGeneratedSids));
            redis.multi()
                .sadd(`SpokenEnglish:CERTIFICATES:${todaysDate}`, ...newGeneratedSids)
                .expire(`SpokenEnglish:CERTIFICATES:${todaysDate}`, HASH_EXPIRY * 7)
                .execAsync();
        }
    } catch (e) {
        console.error(e);
    }
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 */4 * * *", // Every 4 hours
};
