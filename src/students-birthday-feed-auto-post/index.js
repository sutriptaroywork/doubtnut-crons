const moment = require("moment");
const {
    mysql,
    getMongoClient,
} = require("../../modules");

const db = "doubtnut";

async function getUserWhoUHasBirthdayToday() {
    const today = moment().add(5, "hours").add(30, "minutes").format("MM-DD");
    console.log("today", today);
    const format = `%${today}%`;
    /** Getting Students Who have their birthday today */
    const sql = "SELECT student_id,dob,concat(student_fname, ' ', student_lname) as student_name FROM students WHERE  dob != '0000-00-00' and dob IS NOT NULL and dob like ? and student_fname is not null and student_lname is not null";
    return mysql.pool.query(sql, [format]).then((res) => res[0]);
}

async function getUsersConnection(studentId) {
    /** Getting All the followers,followings of user */
    const sql = "SELECT user_id,connection_id  FROM user_connections WHERE user_id = ?";
    return mysql.pool.query(sql, [studentId]).then((res) => res[0]);
}

const msg = "Happy Birthday ";
const happyBirthdayTextImageUrl = "8C280D14-A0CE-B900-38EE-564815B0622A.webp";

async function start(job) {
    try {
        const mongoClient = (await getMongoClient()).db(db);
        job.progress(10);
        await mongoClient.collection("tesla_birthday").updateMany({ }, { $set: { is_active: false } });
        console.log("getting students who have their birthday today");
        const data = await getUserWhoUHasBirthdayToday();
        console.log("data", data);
        job.progress(30);

        for (let i = 0; i < data.length; i++) {
            const userConnections = await getUsersConnection(data[i].student_id);
            const userConnectionsArray = userConnections.map((connection) => connection.connection_id);
            userConnectionsArray.unshift(data[i].student_id);

            await mongoClient.collection("tesla_birthday").insertOne({
                msg: msg.concat(data[i].student_name, " 🎂"),
                type: "image",
                student_id: 98,
                is_deleted: false,
                is_profane: false,
                is_active: true,
                attachment: [happyBirthdayTextImageUrl],
                cdn_url: "https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/",
                visible_to: userConnectionsArray,
            });
        }
        job.progress(90);
        job.progress(100);
        return true;
    } catch (e) {
        throw new Error(e);
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "01 12 * * *",
};
