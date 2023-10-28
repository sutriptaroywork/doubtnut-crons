/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const moment = require("moment");

const {
    mysql, notification, gupshup, deeplink,
} = require("../../modules");

async function getPaymentsForReminder(from, to) {
    const sql = "select p.name, s.mobile, s.locale, s.gcm_reg_id, pi.id, pi.variant_id, pi.updated_at, pi.total_amount, pi.student_id, pi.status from payment_info as pi join students as s on pi.student_id = s.student_id join variants as v on pi.variant_id = v.id join package as p on v.package_id = p.id where pi.id in (select MAX(id) AS id FROM payment_info WHERE source != 'SHIPROCKET' and payment_for='course_package' and updated_at >= ? and updated_at <= ? group by student_id) and (pi.status = \"INITIATED\" or pi.status = \"FAILURE\") order by pi.id desc";
    return mysql.pool.query(sql, [from, to]).then((res) => res[0]);
}

async function getPaymentInfoOnlinePaymentStudentId(studentId) {
    const sql = "select * from payment_info where status = \"SUCCESS\" and student_id = ? and source <> \"SHIPROCKET\" and payment_for <> \"wallet\" and amount <> 0  order by id desc limit 1";
    return mysql.pool.query(sql, [studentId]).then((res) => res[0]);
}

async function getNotificationData(locale, variantId) {
    let title = "Ab aap Cash on Delivery se bhi course le sakte ho🎁🧧";
    let message = "Abhi try karein 🤩";
    const imageUrl = "https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/DD212052-17A5-6A96-78BE-7D92C9072C02.webp";
    if (locale == "hi") {
        title = "अब आप कैश ओन डेलिवेरी से भी कोर्स ले सकते है 🎁🧧";
        message = "अभी दर्ज करें 🤩";
    }

    const data = { variant_id: variantId };
    return {
        notificationData: {
            event: "vip",
            title,
            message,
            image: imageUrl,
            firebase_eventtag: "COD_REMINDER",
            data: JSON.stringify(data),
        },
    };
}

async function start(job) {
    try {
        const from = moment().add(5, "hours").add(30, "minutes").subtract(30, "minutes")
            .format("YYYY-MM-DD HH:mm:ss");
        const to = moment().add(5, "hours").add(30, "minutes").subtract(15, "minutes")
            .format("YYYY-MM-DD HH:mm:ss");
        const paymentsForReminder = await getPaymentsForReminder(from, to);
        console.log("payment reminder", paymentsForReminder);
        for (let i = 0; i < paymentsForReminder.length; i++) {
            const studentId = paymentsForReminder[i].student_id;
            const checkOnlinePayments = await getPaymentInfoOnlinePaymentStudentId(studentId);
            let lastSuccessPaymentTime = 20;
            if (!_.isEmpty(checkOnlinePayments)) {
                lastSuccessPaymentTime = moment().diff(moment(checkOnlinePayments[0].updated_at), "minutes");
            }
            console.log(lastSuccessPaymentTime);
            if (lastSuccessPaymentTime > 15) {
                const variantId = paymentsForReminder[i].variant_id;
                const checkoutDeeplink = `doubtnutapp://vip?variant_id=${variantId}&coupon_code=''`;
                const smsDeeplink = await deeplink.generateDeeplinkFromAppDeeplink("WEB_TO_APP", "ADITYA_USER", checkoutDeeplink);
                // const payload = await getNotificationData(paymentsForReminder[i].locale, variantId);
                // const notificationPayload = payload.notificationData;
                // await notification.sendNotification([{ id: paymentsForReminder[i].student_id, gcmId: paymentsForReminder[i].gcm_reg_id }], notificationPayload);
                gupshup.sendSms({
                    phone: paymentsForReminder[i].mobile,
                    msg: `${paymentsForReminder[i].name} course ke liye aapka payment complete nahi ho paya😔 Dobara payment karne ke liye is link pe click karein 🤗 ${smsDeeplink.url} \n- Team DoubtNut`,
                    locale: paymentsForReminder[i].locale,
                });
            }
        }
        return { err: null, data: null };
    } catch (e) {
        console.log("e1", e);
        return { e };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "2-59/15 * * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
