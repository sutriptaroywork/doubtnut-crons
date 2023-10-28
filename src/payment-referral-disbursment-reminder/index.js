/**
 * This Cro is to send Referral Disbursmet Reminders
 * Of Initiated RazorPay Payouts
 * Only for ₹1000 amount
 */
const moment = require("moment");

const {
    mysql, gupshup, notification,
} = require("../../modules");

async function fetchDisbursmentRemiderData(startDate, endDate) {
    const sql = "select a.payout_url as url, a.mobile, b.student_id, b.locale, b.gcm_reg_id from (select * from student_referral_disbursement where status = 'INITIATED' and source = 'RAZORPAY' and amount = 1000 and updated_at >= ? and updated_at <= ?) as a left join students as b on a.invitor_student_id=b.student_id ";
    return mysql.pool.query(sql, [startDate, endDate]).then((res) => res[0]);
}

async function sendDisbursementReminderCommunication(item) {
    const student = [{
        id: item.student_id,
        gcmId: item.gcm_reg_id,
    }];
    const notificationPayload = {
        event: "external_url",
        title: item.locale == "hi" ? "Doubtnut से आपका ₹1000 CEO ईनाम 💵 असफल हो गया है" : "URGENT : AAPKA ₹1000 💵 CEO CASHBACK FAIL HO GAYA 😢",
        message: item.locale == "hi" ? "UPI या net banking पे पाने के लिए इस पर क्लिक करें" : "UPI ya net banking pe claim karne ke liye click karein",
        firebase_eventtag: "cashback_reminder",
        s_n_id: "REFERRAL_RAZORPAY_REMINDER",
        data: JSON.stringify({
            url: item.url,
        }),
    };
    try {
        await notification.sendNotification(student, notificationPayload);
    } catch (e) {
        console.log(e);
    }

    try {
        console.log(await gupshup.sendSMSMethodGet({
            phone: item.mobile,
            msg: `Doubtnut se aapka ₹1000 CEO referral cash back 💵 fail ho gaya hai
            UPI ya net banking pe claim karne ke liye link par click karein: ${item.url}`,
            locale: "en",
        }));
    } catch (e) {
        console.log(e);
    }
}

async function start(job) {
    try {
        const startDate = moment().add(5, "h").add(30, "minutes").subtract(6, "days")
            .startOf("day")
            .format("YYYY-MM-DD HH:mm:ss");
        const endDate = moment().add(5, "h").add(30, "minutes").subtract(3, "days")
            .startOf("day")
            .format("YYYY-MM-DD HH:mm:ss");
        console.log(startDate, endDate);
        const reminderData = await fetchDisbursmentRemiderData(startDate, endDate);
        console.log(reminderData);
        for (let i = 0; i < reminderData.length; i++) {
            // eslint-disable-next-line no-await-in-loop
            await sendDisbursementReminderCommunication(reminderData[i]);
        }
    } catch (e) {
        console.error(e);
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "00 20 * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
