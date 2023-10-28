const _ = require("lodash");
const moment = require("moment");
const {
    mysql, notification, gupshup, deeplink, kaleyra, slack, redis, config,
} = require("../../modules");

async function set7DData(data) {
    return redis.setAsync("obd_7D_day_data", JSON.stringify(data), "Ex", 60 * 60 * 12);
}
async function setSameDayData(data) {
    return redis.setAsync("obd_same_day_data", JSON.stringify(data), "Ex", 60 * 60 * 12);
}

async function get7DData() {
    return redis.getAsync("obd_7D_day_data");
}
async function get0DData() {
    return redis.getAsync("obd_same_day_data");
}

const smsParams = {
    7: {
        hi: {
            message: `प्रिय छात्र,
आपका कोर्स 7 दिन में ख़तम हो जाएगा। अभी रिचार्ज करो और पाओ ख़ास डिस्काउंट ! - {1}
अगर आपको एडमिशन (दाखिला) टीम से बात करनी है तो 02250573190 पर हमें कॉल करें|
- Team Doubtnut`,
        },
        en: {
            message: `Dear student,
Aapka course 7 din me expire (end) ho jaega. Abhi recharge karo or paao khaas discount! - {1}
Agar aapko admission team se baat karni hai to 02250573190 par humein call karein.
- Team Doubtnut`,
        },
    },

    0: {
        hi: {
            message: `प्रिय छात्र,
आपका कोर्स आज ख़तम हो जाएगा। अभी रिचार्ज करो और पाओ ख़ास डिस्काउंट ! - {1}
अगर आपको एडमिशन (दाखिला) टीम से बात करनी है तो 02250573190 पर हमें कॉल करें
- Team Doubtnut`,
        },
        en: {
            message: `Dear student,
Aapka course aaj expire (end) ho jaega. Abhi recharge karo or paao khaas discount! - {1}
Agar aapko admission team se baat karni hai to 02250573190 par humein call karein, ya {2} par Whatsapp karein.
- Team Doubtnut`,
        },
    },
};
async function getNotificationData(locale, remainingDays, imageUrl, variantId) {
    const couponCode = "LUCKY250";
    let smsMessage = smsParams[remainingDays][locale].message;
    const data = { variant_id: variantId };
    if (couponCode.length > 0) data.coupon_code = couponCode;
    const appDeeplink = `doubtnutapp://vip?variant_id=${variantId}&coupon_code=${couponCode}`;
    const branchDeeplink = await deeplink.generateDeeplinkFromAppDeeplink("SMS", "RENEWAL", appDeeplink);
    smsMessage = smsMessage.replace("{1}", branchDeeplink.url);
    return {
        smsData: {
            message: smsMessage,
            locale,
        },
    };
}

function getUserPackagesByAssortment(studentID, assortmentId) {
    const sql = `select * from (select *,id as subscription_id from student_package_subscription where student_id=${studentID}) as a inner join (select id,assortment_id from package where reference_type in ('v3', 'onlyPanel', 'default') and assortment_id=${assortmentId}) as b on a.new_package_id=b.id`;
    return mysql.pool.query(sql).then(([res]) => res);
}

async function sendMessageAndCall(data) {
    const {
        mobile, smsPayload, studentId,
    } = data;
    await gupshup.sendSms({ phone: mobile, msg: smsPayload.message, locale: smsPayload.locale === "hi" ? "hi" : "en" });
    // console.log(await kaleyra.OBDcall(mobile, audio));
    return studentId;
}

async function getMessageData(data) {
    try {
        // check if its renewal case or not
        const {
            assortment_id: assortmentID, student_id: studentId, remaining_days: remainingDays, locale: localeTemp, gcm_reg_id: gcmId, subscription_id: subscriptionID, mobile,
        } = data;
        const locale = localeTemp === "hi" ? "hi" : "en";
        const allSubscriptionsForAssortment = await getUserPackagesByAssortment(studentId, assortmentID);
        const checkifRenewed = allSubscriptionsForAssortment.filter((e) => e.subscription_id > subscriptionID && e.is_active === 1);
        // let audio;
        // if (remainingDays === 7) {
        //     audio = "163274.ivr";
        // } else if (remainingDays === 0) {
        //     audio = "163330.ivr";
        // }
        if (checkifRenewed.length === 0) {
            const { variant_id: variantId, demo_video_thumbnail: imageUrl } = data;
            const payload = await getNotificationData(locale, remainingDays, imageUrl, variantId);
            const smsPayload = payload.smsData;
            return {
                mobile, smsPayload, studentId,
            };
        }
    } catch (e) {
        console.error(e);
        throw new Error(e);
    }
}
function get7DaysData() {
    const sql = "select a.*, b.*, d.meta_info, d.demo_video_thumbnail, d.assortment_id from (select id as subscription_id, student_id, new_package_id, start_date, end_date, variant_id, datediff(end_date, now()) as remaining_days from student_package_subscription where (end_date <= now() and end_date >= DATE_SUB(NOW(), INTERVAL 7 DAY)) OR (end_date >= now() and end_date <= DATE_ADD(NOW(), INTERVAL 8 DAY) and is_active=1)  and amount > 1 and start_date < now()) as a left join (select student_id, gcm_reg_id, locale, mobile from students) as b on a.student_id=b.student_id left join (select * from package) as c on a.new_package_id=c.id left join (select * from course_details) as d on c.assortment_id=d.assortment_id where remaining_days = 7 group by a.student_id  order by a.student_id";
    return mysql.pool.query(sql).then(([res]) => res);
}

function getSameDayData() {
    const sql = "select a.*, b.*, d.meta_info, d.demo_video_thumbnail, d.assortment_id from (select id as subscription_id, student_id, new_package_id, start_date, end_date, variant_id, datediff(end_date, now()) as remaining_days from student_package_subscription where (end_date <= now() and end_date >= DATE_SUB(NOW(), INTERVAL 7 DAY)) OR (end_date >= now() and end_date <= DATE_ADD(NOW(), INTERVAL 8 DAY) and is_active=1)  and amount > 1 and start_date < now()) as a left join (select student_id, gcm_reg_id, locale, mobile from students) as b on a.student_id=b.student_id left join (select * from package) as c on a.new_package_id=c.id left join (select * from course_details) as d on c.assortment_id=d.assortment_id where remaining_days = 0 group by a.student_id order by a.student_id";
    return mysql.pool.query(sql).then(([res]) => res);
}

async function setAllStudentsDataInRedis() {
    const data = await get7DaysData();
    const dataSameDay = await getSameDayData();
    const studentsList = [];

    await Promise.all(data.map(async (item) => {
        studentsList.push(await getMessageData(item));
    }));
    const studentsListFinal = studentsList.filter(Boolean);
    // console.log(studentsListFinal);
    // se
    const studentsListSameDay = [];

    await Promise.all(dataSameDay.map(async (item) => {
        studentsListSameDay.push(await getMessageData(item));
    }));
    const studentsListSameDayFinal = studentsListSameDay.filter(Boolean);
    // console.log(studentsListSameDayFinal);
    await Promise.all([set7DData(studentsListFinal), setSameDayData(studentsListSameDayFinal)]);
    // const sevenDayList = studentsListFinal.map((x) => x.studentId);
    // const sameDayList = studentsListSameDayFinal.map((x) => x.studentId);
    // const blockNew = [];
    // blockNew.push({
    //     type: "section",
    //     text: { type: "mrkdwn", text: "OBD calls that will be sent today 0d" },
    // },
    // {
    //     type: "section",
    //     text: { type: "mrkdwn", text: `*Count*: ${sameDayList.length}` },
    // },
    // {
    //     type: "section",
    //     text: { type: "mrkdwn", text: `*student-id-list*: \`\`\`${sameDayList.join()}\`\`\`` },
    // });
    // await slack.sendMessage("#obd-sent-notification", blockNew, config.RENEWAL_SLACK_AUTH);
    // const blocks = [];
    // blocks.push({
    //     type: "section",
    //     text: { type: "mrkdwn", text: "OBD calls that will be sent today 7d" },
    // },
    // {
    //     type: "section",
    //     text: { type: "mrkdwn", text: `*Count*: ${sevenDayList.length}` },
    // },
    // {
    //     type: "section",
    //     text: { type: "mrkdwn", text: `*student-id-list*: \`\`\`${sevenDayList.join()}\`\`\`` },
    // });
    // await slack.sendMessage("#obd-sent-notification", blocks, config.RENEWAL_SLACK_AUTH);
}

async function getStudentDataToSend() {
    const sevenDayData = JSON.parse(await get7DData());
    const sameDayData = JSON.parse(await get0DData());
    return { sevenDayData, sameDayData };
}
async function start(job) {
    try {
        job.progress(0);
        const currentHour = moment().add(5, "hours").add(30, "minutes").hour();
        // const currentHour = 12;

        if (currentHour === 12) {
            await setAllStudentsDataInRedis();
        }

        if (currentHour >= 12 && currentHour <= 20) {
            const { sevenDayData, sameDayData } = await getStudentDataToSend();
            const limit7D = sevenDayData.length / 9;
            const limit0D = sameDayData.length / 9;
            const offset7D = limit7D * (currentHour - 12);
            const offset0D = limit0D * (currentHour - 12);
            const sevenDayList = sevenDayData.splice(offset7D, limit7D);
            sevenDayList.forEach((x) => sendMessageAndCall(x));
            // const sevenDayStudentList = sevenDayList.map((x) => x.studentId);
            const sameDayList = sevenDayData.splice(offset0D, limit0D);
            sameDayList.forEach((x) => sendMessageAndCall(x));
            // const sameDayStudentList = sameDayList.map((x) => x.studentId);

            // const blocks = [];
            // blocks.push({
            //     type: "section",
            //     text: { type: "mrkdwn", text: "Obd calls sent 7d" },
            // },
            // {
            //     type: "section",
            //     text: { type: "mrkdwn", text: `*Count*: ${sevenDayStudentList.length}` },
            // },
            // {
            //     type: "section",
            //     text: { type: "mrkdwn", text: `*student-id-list*: \`\`\`${sevenDayStudentList.join()}\`\`\`` },
            // });
            // await slack.sendMessage("#obd-sent-notification", blocks, config.RENEWAL_SLACK_AUTH);

            // const blockNew = [];
            // blockNew.push({
            //     type: "section",
            //     text: { type: "mrkdwn", text: "Obd calls sent 0d" },
            // },
            // {
            //     type: "section",
            //     text: { type: "mrkdwn", text: `*Count*: ${sameDayStudentList.length}` },
            // },
            // {
            //     type: "section",
            //     text: { type: "mrkdwn", text: `*student-id-list*: \`\`\`${sameDayStudentList.join()}\`\`\`` },
            // });
            // await slack.sendMessage("#obd-sent-notification", blockNew, config.RENEWAL_SLACK_AUTH);
            job.progress(100);
            console.log(`the script successfully ran at ${new Date()}`);
            return {
                data: {
                    done: true,
                },
            };
        }
    } catch (err) {
        console.log(err);
        return { err };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 19 * * *",
};
