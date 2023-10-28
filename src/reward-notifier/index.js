const moment = require("moment");
const _ = require("lodash");
const { getMongoClient } = require("../../modules");
const mysql = require("../../modules/mysql");
const sms = require("../../modules/messages");

const notScratchedContentEn = ["Dear Student,\nDoubtnut Scratch Card ki ja rahi hai date!\nKaro Scratch, na hone do waste!\nYahan click karein!\n",
    "Dear Student,\nPrize jeetne ka hai last chance!\nScratch karo! Enjoy karo! Sirf Doubtnut par!\nIs link par dekhein!\n",
    "Dear User,\nNa fisalne dein hathon se ye avsar!\nScratch karein card, jeetein prize Doubtnut ki taraf se!\nIs link par dekhein!\n",
    "Dear Student,\nInaam kar raha hai aapka intezar!\nDoubtnut Scratch card karein Scratch aur enjoy karein!\nYahan click karein!\n",
    "Dear User,\nAapka samay nikalta jaa raha hai!\nKaro Cad Scratch, Doubtnut ki taraf se jeeto inaam!\nIs link par dekhein!\n"];
const notScratchedContentHi = ["प्रिय यूज़र, घड़ी की टिक-टिक दिला रही है कुछ याद!\nडाउटनट पर जाकर स्क्रैच करो अपना कार्ड!\nयहाँ क्लिक करें!\n",
    "प्रिय स्टूडेंट, डाउटनट स्क्रैच कार्ड की जा रही है डेट!\nकरो स्क्रैच, न होने दो वेस्ट!\nयहाँ क्लिक करें!\n",
    "प्रिय स्टूडेंट, इनाम जीतने का है आखिरी चांस!\nस्क्रैच करो! एन्जॉय करो! सिर्फ डाउटनट पर!\nइस लिंक पर देखें!\n",
    "प्रिय स्टूडेंट, न फिसलने दें हाथों से ये अवसर!\nस्क्रैच करें कार्ड, जीतें इनाम डाउटनट की तरफ से!\nइस लिंक पर देखें!\n",
    "प्रिय यूज़र, इनाम कर रहा है आपका इंतज़ार!\nडाउटनट स्क्रैच कार्ड करें स्क्रैच और एन्जॉय करें!\nयहाँ क्लिक करें!\n",
    "प्रिय यूज़र, आपका समय निकलता जा रहा है!\nकरो कार्ड स्क्रैच, डाउटनट की तरफ से जीतो इनाम!\nइस लिंक पर देखें!\n"];

class RewardNotifier {
    constructor(client) {
        this.notification_date = new Date(moment().add(-1, "days").format("YYYY-MM-DD"));
        this.client = client;
    }

    async main(job) {
        console.log("job create notification");
        await job.progress(11);
        let i; let j; let temparray; const
            chunk = 10000;
        let skip = 0;
        for (i = 0, j = 5000000; i < j; i += chunk) {
            const nonScractchedStudents = await this.getApplicableStudents(skip, chunk);
            if (!nonScractchedStudents) {
                console.log("No more data left");
                break;
            }
            const studentDetails = await this.getStudentDetails(nonScractchedStudents);
            await this.sendSms(studentDetails);
            skip += chunk;
            console.log(skip, " skip");
        }
        await job.progress(90);
    }

    async getApplicableStudents(skip, limit) {
        const nonScractchedStudents = await this.client.collection("student_rewards").find({
            is_notification_opted: true,
            scratch_cards: { $elemMatch: { is_scratched: false, unlocked_at: { $gt: this.notification_date } } },
        }).skip(skip).limit(limit)
            .toArray();

        const studentIds = [];
        for (const r of nonScractchedStudents) {
            studentIds.push(r.studentId);
        }
        console.log(studentIds.length, " total users");
        return studentIds;
    }

    async getStudentDetails(nonScractchedStudents) {
        const sql = "SELECT student_id, gcm_reg_id, mobile, locale FROM students WHERE student_id in (?)";
        const result = await mysql.pool.query(sql, [nonScractchedStudents])
            .then((res) => res[0]);
        console.log(result);
        return result;
    }

    async sendSms(studentDetails) {
        for (const student of studentDetails) {
            if (student.mobile) {
                const message = `${(student.locale === "hi" ? _.sample(notScratchedContentHi) : _.sample(notScratchedContentEn))}https://tinyurl.com/2zr66n96`;
                console.log("Sending SMS");
                sms.sendSms({ phone: student.mobile, msg: message, locale: student.locale });
            }
        }
        console.log("message sent");
    }
}

async function start(job) {
    job.progress(10);
    console.log("task started");
    const client = (await getMongoClient()).db("doubtnut");
    const notification = new RewardNotifier(client);
    await notification.main(job);
    await job.progress(100);
    console.log("task completed");
    return { data: "success" };
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 05 11 * * *",
};
