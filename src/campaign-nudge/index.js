/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const moment = require("moment");
const {
    mysql, redis, config, notification,
} = require("../../modules");

async function fetchUsers() { // 45 ms
    const sql = "select student_id,gcm_reg_id,udid, locale, timestamp, app_version from students where timestamp < (NOW() - INTERVAL 1 HOUR) and timestamp > (NOW() - INTERVAL 2 HOUR) and is_web=0 and gcm_reg_id is not null and gcm_reg_id <> ''";
    // console.log(sql);
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getRedirectionDetailsFromCampaign(campaign) {
    const sql = `select * from (select * from campaign_redirection_mapping where campaign='${campaign}') as a left join sticky_notification as b on a.sticky_notification_id=b.id`;
    // console.log(sql);
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function checkBoughtAssortment(studentId, assortmentId) {
    const sql = `select * from (select student_id, new_package_id from student_package_subscription where student_id=${studentId} and end_date >= CURRENT_DATE) as a inner join (select id, assortment_id from package) as b on a.new_package_id=b.id where b.assortment_id=${assortmentId}`;
    // console.log(sql);
    return mysql.pool.query(sql).then((res) => res[0]);
}
function getNotificationPayload(notificationData, assortmentData, payloadType, assortmentId) {
    let notifdata;
    if (payloadType === "text") {
        notifdata = {
            id: notificationData.id,
            image_url: `${config.staticCDN}engagement_framework/170A60A2-62D4-FB41-459B-9A64D2F2DE15.webp`,
            is_vanish: notificationData.is_vanish !== "0",
            deeplink_banner: notificationData.deeplink_banner,
            offset: notificationData.offset * 1000,
        };
    } else if (payloadType === "banner") {
        notifdata = {
            id: notificationData.id,
            image_url: notificationData.image_url,
            is_vanish: notificationData.is_vanish !== "0",
            deeplink_banner: notificationData.deeplink_banner,
            offset: +notificationData.offset * 1000,
        };
    } else {
        notifdata = {
            id: notificationData.id,
            image_url: notificationData.image_url,
            is_vanish: notificationData.is_vanish !== "0",
            price: `₹ ${assortmentData[assortmentId].display_price}`,
            price_color: "#efefef",
            crossed_price: `₹ ${assortmentData[assortmentId].base_price}`,
            crossed_price_color: "#d9d7d7",
            cross_color: "#d9d7d7",
            text: notificationData.text_under_price,
            text_color: "#efefef",
            button_cta: notificationData.button_cta,
            button_text_color: "#ffffff",
            button_background_color: "#ea532c",
            deeplink_banner: notificationData.deeplink_banner,
            deeplink_button: notificationData.deeplink_button,
            offset: +notificationData.offset * 1000,
        };
    }
    const date = moment().add(5, "hours").add(30, "minutes").format("YYYY-MM-DD");
    return {
        event: "course_notification",
        firebase_eventtag: "course_notification",
        sn_type: payloadType,
        s_n_id: `announcement-${date}`,
        title: (payloadType === "text") ? notificationData.title_text_notification : notificationData.title_image_notification,
        message: (payloadType === "text") ? notificationData.message_text_notification : notificationData.message_image_notification,
        data: notifdata,
    };
}
async function getDefaultVariantFromAssortmentIdHome(assortmentId) {
    const mysqlQ = `select * from (select id, type, assortment_id, name, description, duration_in_days from package where assortment_id=${assortmentId} and flag_key is null and reference_type='v3' and type='subscription') as a inner join (select id as variant_id, package_id, base_price, display_price from variants where is_default=1) as b on a.id=b.package_id order by a.duration_in_days`;
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}
async function generateAssortmentVariantMapping(assortmentId) {
    const flagIds = [await getDefaultVariantFromAssortmentIdHome(assortmentId)];
    const enabled = true;
    const assortmentPriceMapping = {};
    const assortmentList = [];
    for (let i = 0; i < flagIds.length; i++) {
        if (flagIds[i].length) {
            const len = flagIds[i].length;
            const priceObj = enabled ? flagIds[i][0] : flagIds[i][len - 1];
            assortmentPriceMapping[parseInt(priceObj.assortment_id)] = {
                package_variant: priceObj.variant_id,
                base_price: priceObj.base_price,
                display_price: priceObj.display_price,
                duration: priceObj.duration_in_days,
            };
            assortmentList.push(flagIds[i][0].assortment_id);
            if (flagIds[i].length > 1) {
                assortmentPriceMapping[parseInt(flagIds[i][0].assortment_id)].multiple = true;
                assortmentPriceMapping[parseInt(flagIds[i][0].assortment_id)].enabled = enabled;
            }
        }
    }
    return assortmentPriceMapping;
}
async function sendStickyNotification(notificationData, assortmentId, userDetails) {
    const assortmentData = await generateAssortmentVariantMapping(assortmentId);
    // console.log(assortmentData);
    const splittedAppVersion = userDetails.app_version.split(".");
    if (splittedAppVersion[2] > 229) {
        const payloadType = "image";
        const payload = getNotificationPayload(notificationData, assortmentData, payloadType, assortmentId);
        await notification.sendNotification([{ id: userDetails.student_id, gcmId: userDetails.gcm_reg_id }], payload);
        // await notification.sendNotification([{ id: 2524641, gcmId: "fPC-rOStRRmqQzTP9jVC0G:APA91bHzPHm7afF5fuYJ0p3IygnadSCbV_S3KRdDQWEvr4QcGaLz2s--luCRyME5Cs0AMgwVYjcoFYXo1RaJyDn6e2RU0G-4z382ZhJM2PBgy1-DJyVMGHt2mBV9j2YmuX7m5_KMtfLL" }], payload);
    }
}
async function start(job) {
    try {
        const students = await fetchUsers();
        // console.log("students");
        // console.log(students.length);
        // console.log("branch:2021-08-24");
        for (let i = 0; i < students.length; i++) {
            // console.log(students[i]);
            const { udid } = students[i];

            // check campaign
            let campaignData = await redis.hgetAsync(`branch:${moment().add(5, "hour").add(30, "minutes").format("YYYY-MM-DD")}`, `udid_${udid}`);
            if (_.isNull(campaignData)) {
                campaignData = await redis.hgetAsync(`branch:${moment().add(5, "hour").add(30, "minutes").subtract(1, "day")
                    .format("YYYY-MM-DD")}`, `udid_${udid}`);
            } // console.log("campaignData");
            // console.log(`udid_${udid}`);
            // console.log(campaignData);
            if (!_.isNull(campaignData)) {
                // console.log("campaignData");
                // console.log(`udid_${udid}`);
                // console.log(campaignData);
                // console.log(students[i].student_id);
                campaignData = campaignData.split("::")[1];
                // get deeplink from campaign name
                const deeplinkDetails = await getRedirectionDetailsFromCampaign(campaignData);
                // console.log(deeplinkDetails);
                if (deeplinkDetails.length > 0) {
                    if (deeplinkDetails[0].deeplink.includes("course_details")) {
                        const check = await checkBoughtAssortment(students[i].student_id, deeplinkDetails[0].deeplink.split("=")[1]);
                        // console.log("check");
                        // console.log(check);
                        if (check.length === 0) {
                            // did not bought course; target the notification
                            // eslint-disable-next-line no-unused-vars
                            const result = await sendStickyNotification(deeplinkDetails[0], deeplinkDetails[0].deeplink.split("=")[1], students[i]);
                            // console.log("result");
                            // console.log(result);
                        }
                    }
                }
            }
        }
        job.progress(100);
    } catch (e) {
        console.log("error");
        console.log(e);
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 * * * *",
    removeOnComplete: 10,
    removeOnFail: 20,
};
