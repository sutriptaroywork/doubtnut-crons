/* eslint-disable no-await-in-loop */
const moment = require("moment");
const _ = require("lodash");
const {
    mysql, gupshup, deeplink, notification,
} = require("../../modules");

async function fetchStudents() {
    const sql = "select a.*, b.assortment_id, d.gcm_reg_id, c.meta_info, d.mobile, c.demo_video_thumbnail, e.display_price, c.parent from (select * from student_package_subscription where start_date < CURRENT_DATE and end_date >= CURRENT_DATE and amount <> -1 and is_active=1 and new_package_id is not null and variant_id is not null and updated_by NOT LIKE 'AS DNK OLD PACKAGE MIGRATION' ) as a left join package as b on a.new_package_id=b.id left join  (select * from course_details where assortment_type in ('class', 'course')) as c on b.assortment_id=c.assortment_id left join students as d on a.student_id=d.student_id left join variants as e on a.variant_id=e.id left join internal_subscription as f on a.student_id=f.student_id where f.student_id is null and c.assortment_id is not null ";
    // const sql = `select * from students `
    console.log(sql);
    return mysql.pool.query(sql).then(([res]) => res);
}

async function fetchMigratedStudents() {
    const sql = "select a.*, b.assortment_id, d.gcm_reg_id, c.meta_info, d.mobile, c.demo_video_thumbnail, e.display_price, c.parent from (select * from student_package_subscription where start_date < CURRENT_DATE and end_date >= CURRENT_DATE and amount <> -1 and is_active=1 and new_package_id is not null and variant_id is not null and updated_by LIKE 'AS DNK OLD PACKAGE MIGRATION') as a left join package as b on a.new_package_id=b.id left join  (select * from course_details where assortment_type in ('class', 'course')) as c on b.assortment_id=c.assortment_id left join students as d on a.student_id=d.student_id left join variants as e on a.variant_id=e.id left join internal_subscription as f on a.student_id=f.student_id where f.student_id is null and c.assortment_id is not null group by a.student_id, c.class, c.meta_info";
    // const sql = `select * from students `
    console.log(sql);
    return mysql.pool.query(sql).then(([res]) => res);
}

async function getQuestionIdsFromAssortmentId(assortment_id) {
    const sql = `select b.resource_reference as question_id from (select course_resource_id,assortment_id,live_at from course_resource_mapping where assortment_id in (select course_resource_id from course_resource_mapping where assortment_id in (select course_resource_id from course_resource_mapping where assortment_id in (select course_resource_id from course_resource_mapping where assortment_id in (${assortment_id}) and resource_type='assortment') and resource_type='assortment') and resource_type='assortment') and resource_type='resource') as a inner join (select id,meta_info,resource_reference from course_resources where resource_type in (1,4,8)) as b on b.id=a.course_resource_id`;
    // console.log(sql);
    return mysql.pool.query(sql).then(([res]) => res);
}

async function getVideoViews(data) {
    const sql = `select student_id, created_at, CAST(question_id AS CHAR(50)) as question_id from video_view_stats where student_id=${data.student_id} and created_at >= '${data.start_date}' order by view_id DESC LIMIT 1000`;
    console.log(sql);
    return mysql.pool.query(sql).then(([res]) => res);
}
const postPurchaseCommunication = {
    inactiveSincePurchase: {
        notification: {
            en: {
                title: "Apne Rs.{1} barbaad mat hone dein 💰🤑",
                message: "{2} din pehle course khareeda par ab tak koi class nahi lagaya!",
                firebaseTag: "INACTIVE_SINCE_PURCHASE_EN",
            },
            hi: {
                title: "अपने {1} रुपये बर्बाद न होने दें 💰🤑",
                message: "{2} दिन पहले कोर्स खरीदा पर अब तक कोई क्लास नहीं लगाया!ं",
                firebaseTag: "INACTIVE_SINCE_PURCHASE_HI",
            },
        },
        SMS: {
            en: {

                message: "Apne Rs.{1} barbaad na hone dein!\n{2} din pehle course khareeda par ab tak koi class nahi lagaya!\n\nDoubtnut par abhi padhai chalu karein! - {3}",
            },
            hi: {
                message: "अपने {1} रुपये बर्बाद न होने दें!\n{2} दिन पहले कोर्स खरीदा पर अब तक कोई क्लास नहीं लगाया!\nDoubtnut पर अभी पढ़ाई चालू करें! - {3}",
            },
        },
    },
    postPurchaseInactivity: {
        notification: {
            en: {
                title: "APNI HI CLASS SE GAYAB?",
                message: "3 dino se aapne apni class attend nahin ki hai. Abhi jaayein or class lagana wapas shuru karein!",
                firebaseTag: "POST_PURCHASE_INACTIVITY_EN",
            },
            hi: {
                title: "अपनी ही क्लास से गायब?",
                message: "3 दिनों से आपने अपनी क्लास नहीं देखी है| अभी जाएँ और अपनी क्लास लगाना वापस शुरू करें!",
                firebaseTag: "POST_PURCHASE_INACTIVITY_HI",
            },
        },
        SMS: {
            en: {
                message: "APNI HI CLASS SE GAYAB? 3 dino se aapne apni class attend nahin ki hai!\nAbhi jaayein or DOUBTNUT par classes lagana wapas shuru karein. {1}",
            },
            hi: {
                message: "अपनी ही क्लास से गायब? 3 दिनों से आपने अपनी क्लास नहीं देखी है| अभी जाएँ और Doubtnut पर अपनी क्लास लगाना वापस शुरू करें! {1}",
            },
        },
    },
};

async function processStudent(assortmentQuestions, student) {
    if (!assortmentQuestions[student.assortment_id] || !assortmentQuestions[student.assortment_id].length) {
        return;
    }
    try {
        const startDate = student.start_date;
        if (moment().subtract(1, 'M').diff(moment(student.start_date), 'days') >= 0) {
            student.start_date = moment().subtract(1, 'M').format("YYYY-MM-DD HH:mm:ss");
        } else {
            student.start_date = moment(student.start_date).format("YYYY-MM-DD HH:mm:ss");
        }
        const videoViews = await getVideoViews(student);
        student.start_date = startDate;
        const intersection = _.intersectionBy(videoViews, assortmentQuestions[student.assortment_id], "question_id");
        if (!intersection.length) {
            // send notification
            const stu = {};
            stu.id = student.student_id;
            stu.gcmId = student.gcm_reg_id;
            const diffDays = moment().diff(moment(student.start_date), "days");
            const notificationPayload = {
                event: "course_details",
                image: (student && student.demo_video_thumbnail) ? student.demo_video_thumbnail : "",
                title: postPurchaseCommunication.inactiveSincePurchase.notification[student.meta_info == "HINDI" ? "hi" : "en"].title.replace("{1}", student.display_price),
                message: postPurchaseCommunication.inactiveSincePurchase.notification[student.meta_info == "HINDI" ? "hi" : "en"].message.replace("{1}", student.display_price).replace("{2}", diffDays),
                firebase_eventtag: postPurchaseCommunication.inactiveSincePurchase.notification[student.meta_info == "HINDI" ? "hi" : "en"].firebaseTag,
                s_n_id: postPurchaseCommunication.inactiveSincePurchase.notification[student.meta_info == "HINDI" ? "hi" : "en"].firebaseTag,
                // data: JSON.stringify({
                //     id: student.assortment_id,
                // }),
            };
            if (student.parent == 4) {
                notificationPayload.event = "course_category";
                notificationPayload.data = JSON.stringify({
                    category_id: "Kota Classes",
                });
            } else {
                notificationPayload.data = JSON.stringify({
                    id: student.assortment_id,
                });
            }
            notification.sendNotification([stu], notificationPayload);
            // deeplink.generateDeeplinkFromAppDeeplink("SMS", "INACTIVE_SINCE_PURCHASE", student.parent == 4 ? "doubtnutapp://course_category?category_id=Kota Classes" : `doubtnutapp://course_details?id=${student.assortment_id}&referrer_student_id=`).then((branchDeeplink) => {
            //     gupshup.sendSms({ phone: student.mobile, msg: postPurchaseCommunication.inactiveSincePurchase.SMS[student.meta_info == "HINDI" ? "hi" : "en"].message.replace("{1}", student.display_price).replace("{2}", diffDays).replace("{3}", branchDeeplink.url), locale: student.meta_info === "HINDI" ? "hi" : "en" });
            // });
        } else {
            // send notification
            // sort intersection array according to created_at
            // const myOrderedArray = _.orderBy(intersection, (a) => moment(a.created_at), "DESC");
            // const diffDays = moment().diff(moment(intersection[0].created_at), "days");
            const diffDays = moment().diff(moment(intersection[0].created_at), "days");
            if (diffDays != 0 && diffDays % 3 == 0) {
                // send notification
                const stu = {};
                stu.id = student.student_id;
                stu.gcmId = student.gcm_reg_id;
                const notificationPayload = {
                    event: "course_details",
                    image: (student && student.demo_video_thumbnail) ? student.demo_video_thumbnail : "",
                    title: postPurchaseCommunication.postPurchaseInactivity.notification[student.meta_info == "HINDI" ? "hi" : "en"].title,
                    message: postPurchaseCommunication.postPurchaseInactivity.notification[student.meta_info == "HINDI" ? "hi" : "en"].message,
                    firebase_eventtag: postPurchaseCommunication.postPurchaseInactivity.notification[student.meta_info == "HINDI" ? "hi" : "en"].firebaseTag,
                    s_n_id: postPurchaseCommunication.postPurchaseInactivity.notification[student.meta_info == "HINDI" ? "hi" : "en"].firebaseTag,
                    // data: JSON.stringify({
                    //     id: student.assortment_id,
                    // }),
                };
                if (student.parent == 4) {
                    notificationPayload.event = "course_category";
                    notificationPayload.data = JSON.stringify({
                        category_id: "Kota Classes",
                    });
                } else {
                    notificationPayload.data = JSON.stringify({
                        id: student.assortment_id,
                    });
                }
                notification.sendNotification([stu], notificationPayload);
                // deeplink.generateDeeplinkFromAppDeeplink("SMS", "POST_PURCHASE_INACTIVITY", student.parent == 4 ? "doubtnutapp://course_category?category_id=Kota Classes" : `doubtnutapp://course_details?id=${student.assortment_id}&referrer_student_id=`).then((branchDeeplink) => {
                //     gupshup.sendSms({ phone: student.mobile, msg: postPurchaseCommunication.postPurchaseInactivity.SMS[student.meta_info == "HINDI" ? "hi" : "en"].message.replace("{1}", branchDeeplink.url), locale: student.meta_info === "HINDI" ? "hi" : "en" });
                // });
            }
        }
    } catch (e) {
        console.error(e);
    }
}

async function start(job) {
    try {
        const assortmentQuestions = {};
        let students = await fetchStudents();
        // console.log(students.length);
        const migratedStudents = await fetchMigratedStudents();
        students = _.concat(students, migratedStudents);
        // console.log(students.length);
        for (let i = 0; i < students.length; i++) {
            if (!assortmentQuestions[students[i].assortment_id]) {
                const assQuestions = await getQuestionIdsFromAssortmentId(students[i].assortment_id);
                assortmentQuestions[students[i].assortment_id] = assQuestions;
            }
        }
        let c = 0;
        while (students.length) {
            const s = students.splice(0, 100);
            await Promise.all(s.map((x) => processStudent(assortmentQuestions, x)));
            c += 100;
            await job.progress(parseInt(((c + 1) * 100) / students.length));
        }
        return { data: null };
    } catch (e) {
        console.log(e);
        return { err: e };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 8 * * *",
    removeOnComplete: 10,
    removeOnFail: 20,
};
