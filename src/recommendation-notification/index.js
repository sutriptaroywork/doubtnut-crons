/* eslint-disable no-await-in-loop */
const Redis = require("ioredis");
const bluebird = require("bluebird");
const _ = require("lodash");

const { mysql, notification } = require("../../modules");

bluebird.promisifyAll(Redis);

function getNotificationPayload(getVisitedAssortmentToSend, assortmentData, assortmentImage) {
    const isVanish = "1";
    const deeplink = `doubtnutapp://course_details?id=${getVisitedAssortmentToSend[0].assortment_id}`;
    const deeplink2 = `doubtnutapp://vip?variant_id=${assortmentData[getVisitedAssortmentToSend[0].assortment_id].package_variant}`;
    const notifdata = {
        id: `${getVisitedAssortmentToSend[0].id}`,
        image_url: assortmentImage[0].image_url,
        is_vanish: isVanish !== "0",
        price: `₹ ${assortmentData[getVisitedAssortmentToSend[0].assortment_id].display_price}`,
        price_color: "#efefef",
        crossed_price: `₹ ${assortmentData[getVisitedAssortmentToSend[0].assortment_id].base_price}`,
        crossed_price_color: "#d9d7d7",
        cross_color: "#d9d7d7",
        text: "Per month",
        text_color: "#efefef",
        button_cta: "Buy Now",
        button_text_color: "#ffffff",
        button_background_color: "#ea532c",
        deeplink_banner: deeplink,
        deeplink_button: deeplink2,
        offset: 86400 * 1000,
    };
    return {
        event: "course_notification",
        firebase_eventtag: "recommendation_notification",
        sn_type: "image",
        s_n_id: "recommendation-sticky",
        title: "Course Recommendation",
        message: "Sticky Notification",
        data: notifdata,
    };
}

async function getStudentData(students) {
    const mysqlQ = `select student_id,is_online,gcm_reg_id from students where student_id in (${students})`;
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}

async function getUserActivePackages(studentId) {
    const mysqlQ = `select * from (select *, id as subscription_id from student_package_subscription where student_id=${studentId} and start_date < now() and end_date > now() and is_active=1 order by id desc) as a inner join (select * from package where reference_type in (\'v3\', \'onlyPanel\', \'default\')) as b on a.new_package_id=b.id left join (select class,assortment_id, assortment_type,display_name, year_exam,display_description,category,meta_info from course_details) as cd on cd.assortment_id=b.assortment_id group by cd.assortment_id order by a.id desc`;
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}

async function getDefaultVariantFromAssortmentIdHome(assortmentId) {
    const mysqlQ = `select * from (select id, type, assortment_id, name, description, duration_in_days from package where assortment_id=${assortmentId} and flag_key is null and reference_type=\'v3\' and type=\'subscription\') as a inner join (select id as variant_id, package_id, base_price, display_price from variants where is_default=1) as b on a.id=b.package_id order by a.duration_in_days`;
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}

async function getBanner(assortmentId, type, versionCode) {
    const mysqlQ = `select image_url, assortment_id as course_thumbnail_assortment_id from course_details_thumbnails where assortment_id=${assortmentId} and is_active = 1 and type = '${type}' and min_version_code <= ${versionCode} and max_version_code >= ${versionCode}`;
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}

async function updateRecommendationStickyNotification(sentStudents) {
    const sql = `update recommendation_sticky_notification set is_sent = 1 where student_id in (${sentStudents})`;
    return mysql.writePool.query(sql).then((res) => res);
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

async function notificationNotProcessedAction(allStudentsData) {
    try {
        const uniqueAllStudent = [...new Set(allStudentsData.map((item) => item.student_id))];
        const studentData = await getStudentData(uniqueAllStudent);
        const uniqueStudents = [...new Set(studentData.map((item) => item.student_id))];
        const type = "widget_popular_course";
        const versionCode = 900;
        const workers = [];
        const sentStudents = [];
        const chunkSize = 100;
        for (let e = 0, f = uniqueStudents.length; e < f; e += chunkSize) {
            const uniqueStudentsSlice = uniqueStudents.slice(e, e + chunkSize);
            for (let i = 0; i < uniqueStudentsSlice.length; i++) {
                const index = studentData.map((item) => item.student_id).indexOf(uniqueStudentsSlice[i]);
                if (studentData[index] && studentData[index].is_online && studentData[index].gcm_reg_id) {
                    const appVersion = studentData[index].is_online;
                    if (appVersion >= 878) {
                        const notificationObject = {
                            id: studentData[index].student_id,
                            gcmId: studentData[index].gcm_reg_id,
                        };
                        const getVisitedAssortments = allStudentsData.filter((item) => item.student_id == uniqueStudentsSlice[i]);
                        // eslint-disable-next-line no-await-in-loop
                        const studentSubscriptionDetails = await getUserActivePackages(uniqueStudentsSlice[i]);
                        const courseSubscriptions = studentSubscriptionDetails.filter((item) => item.assortment_type == "course");
                        if (getVisitedAssortments && getVisitedAssortments[0]) {
                            const getVisitedAssortmentToSend = getVisitedAssortments.filter((item) => courseSubscriptions.every((item2) => item2.assortment_id !== item.assortment_id));
                            if (getVisitedAssortmentToSend && getVisitedAssortmentToSend[0]) {
                                // eslint-disable-next-line no-await-in-loop
                                const assortmentData = await generateAssortmentVariantMapping(getVisitedAssortmentToSend[0].assortment_id);
                                // eslint-disable-next-line no-await-in-loop
                                const assortmentImage = await getBanner(getVisitedAssortmentToSend[0].assortment_id, type, versionCode);
                                if (!_.isEmpty(assortmentData) && assortmentImage && assortmentImage[0]) {
                                    const payload = getNotificationPayload(getVisitedAssortmentToSend, assortmentData, assortmentImage);
                                    workers.push(notification.sendNotification([notificationObject], payload));
                                }
                                sentStudents.push(uniqueStudentsSlice[i]);
                            }
                        }
                    }
                }
            }
            await Promise.all(workers);
            await updateRecommendationStickyNotification(uniqueAllStudent);
        }
    } catch (e) {
        console.log(e);
        throw new Error("Error in sticky notification");
    }
}

async function getStickyNotification() {
    const mysqlQ = "select * from recommendation_sticky_notification where is_sent = 0 order by created_at desc";
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}

async function start(job) {
    try {
        const stickyNotificationData = await getStickyNotification();
        await notificationNotProcessedAction(stickyNotificationData);
        job.progress(100);
        console.log(`the script successfully ran at ${new Date()}`);
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
    cron: "*/30 * * * *",
};
