const _ = require("lodash");
const moment = require("moment");
const messages = require("../../modules/messages");
const {
    mysql,
    getMongoClient,
} = require("../../modules");

const db = "doubtnut";
/** This cron is to send sms to user who have have installed and uninstalled app the same day */

const smsContent = {
    en: "Doubtnut App ko Dobara Install kare aur paaye Paytm,Myntra,Jio jaise brands ke vouchers bilkul FREE. Toh clear kare apne doubts and Jeete Free rewards. Abhi Install kare - https://bit.ly/2LQLVU8",
    hi: "डाउटनट ऐप को दोबारा इंस्टॉल करें करे और पाए पेटीएम, मिंत्रा, जियो जैसे ब्रांड के वाउचर बिलकुल फ्री। तो क्लियर करें अपने डाउट और जीतें फ्री रिवार्ड्स। अभी इंस्टाल करे - https://bit.ly/2LQLVU8",
    other: "Install the Doubtnut app again and get a chance to earn vouchers from Paytm,Myntra and Jio for Free. So clear your doubts and earn rewards. Install Doubtnut app now-https://bit.ly/2LQLVU8",
};

async function getSameDayUninstalledUsers() {
    /** Getting Students Who installed and uninstalled app the same day */
    // const sql = `SELECT distinct b.locale, b.mobilefrom (SELECT to_date(curtimestamp + interval '330 minutes', 'YYYY|MM|DD') as join_dt, student_id,locale,mobile from classzoo1.students where is_web <> '1' AND mobile is not null
    // and curtimestamp + interval '330 minutes' >= CURRENT_DATE - INTERVAL '1 DAY' and curtimestamp + interval '330 minutes' < CURRENT_DATE) as bLEFT JOIN (SELECT to_date(uninstall_timestamp, 'YYYY|MM|DD') as uninstall_dt, student_id from classzoo1.retarget_student_churn
    //  where reinstall_timestamp is NULL and uninstall_timestamp >= CURRENT_DATE - INTERVAL '1 DAYS' and uninstall_timestamp < CURRENT_DATE) AS c ON b.student_id = c.student_idand b.join_dt = c.uninstall_dt
    //             where c.student_id is not null`;
    const sql = "SELECT DISTINCT b.locale, b.mobile, b.student_id from (SELECT DATE(timestamp) as join_dt, student_id,locale,mobile from students where is_web <> '1' AND mobile is not null and timestamp >= DATE_SUB(CURDATE(),INTERVAL 1 DAY) and timestamp < CURDATE()) as b LEFT JOIN (SELECT DATE(uninstall_timestamp) as uninstall_dt, student_id from retarget_student_churn where reinstall_timestamp is NULL and uninstall_timestamp >= DATE_SUB(CURDATE(),INTERVAL 1 DAY)  and uninstall_timestamp < CURDATE()) AS c ON b.student_id = c.student_id and b.join_dt = c.uninstall_dt where c.student_id is not null";
    const users = await mysql.pool.query(sql).then((res) => res[0]);
    return users;
}

async function userIsAlreadyAwarded(mongoClient, studentId) {
    const userAwardData = await mongoClient.collection("reinstall_rewarded_students").findOne({ studentId });
    return userAwardData;
}

async function start(job) {
    const mongoClient = (await getMongoClient()).db(db);
    const allowedLocales = ["hi", "en"];
    job.progress(10);
    console.log("getting students who installed and uninstalled the app today");
    const data = await getSameDayUninstalledUsers();
    job.progress(30);
    let locale = "other";
    let totalSent = 0;
    let totalNotSent = 0;
    for (let i = 0; i < data.length; i++) {
	    const user = data[i];
        try {
            if (allowedLocales.includes(user.locale)) {
                locale = user.locale;
            }
            // checking if user has already got
            const isAlreadyAwarded = await userIsAlreadyAwarded(mongoClient, user.student_id);
            if (_.isEmpty(isAlreadyAwarded)) {
                await messages.sendSms({
                    phone: user.mobile,
                    msg: smsContent[locale],
                    locale,
                });
                await mongoClient.collection("reward_sms_sent_students").insertOne({
                    student_id: user.student_id,
                    mobile: user.mobile,
                    createdAt: moment().add(5, "hours").add(30, "minutes").toDate(),
                });
            }
            totalSent += 1;
            console.log("SENT on => ", user.mobile, user.locale);
        } catch (e) {
            totalNotSent += 1;
            console.log("msg not sent");
        }
    }
    job.progress(90);
    await messages.sendSms({
        phone: 9682632079,
        msg: `Total Sent: ${totalSent}\nTotal Not Sent: ${totalNotSent}\nGoogle Form Target SMS Sent Today`,
    });
    await messages.sendSms({
        phone: 8699616342,
        msg: `Total Sent: ${totalSent}\nTotal Not Sent: ${totalNotSent}\nGoogle Form Target SMS Sent Today`,
    });
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "30 8 * * *",
};
