/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const moment = require("moment");
const {
    mysql, kafka,
} = require("../../modules");

async function getUsers() {
    const sql = `select a.display_name, b.assortment_id, c.student_id, d.gcm_reg_id, d.locale from (select * from course_details where start_date >= '${moment().format("YYYY-MM-DD")}  00:00:00' and start_date < '${moment().add(1, "days").format("YYYY-MM-DD")}  00:00:00')  as a inner join (select * from package) as b on a.assortment_id=b.assortment_id inner join (select * from student_package_subscription where start_date < now() and end_date > now() and is_active=1  and amount = -1) as c on b.id=c.new_package_id inner join (select * from students where gcm_reg_id is not null) as d on c.student_id=d.student_id`;
    console.log(sql);
    return mysql.pool.query(sql).then((res) => res[0]);
}
function getNotificationPayload(locale, courseName, assortmentID) {
    const title = courseName;
    let message = "Your free trial starts today !";
    let imageUrl = "https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/1504F241-C872-0B1F-2D77-75FF21356BF5.webp";
    if (locale === "hi") {
        message = "आप का फ़्री ट्रायल आज से शुरू";
        imageUrl = "https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/979FC27F-71E8-3BFB-E7B6-20E6FE8A8C4B.webp";
    }
    return {
        event: "course_details",
        title,
        message,
        image: imageUrl,
        firebase_eventtag: `COURSE_START_${assortmentID}`,
        data: JSON.stringify({ id: assortmentID }),
    };
}

async function start(job) {
    try {
        job.progress(0);
        const userDetails = await getUsers();
        const groupedUser = _.groupBy(userDetails, "assortment_id");
        for (const key in groupedUser) {
            if (Object.prototype.hasOwnProperty.call(groupedUser, key)) {
                const items = groupedUser[key];
                const hindiUsersList = [];
                const hindiGcmList = [];
                const englishUsersList = [];
                const englishGcmList = [];
                const hindiPayload = getNotificationPayload("hi", items[0].display_name, key);
                const enPayload = getNotificationPayload("en", items[0].display_name, key);
                for (let i = 0; i < items.length; i++) {
                    if (items[i].locale === "hi") {
                        hindiUsersList.push(items[i].student_id);
                        hindiGcmList.push(items[i].gcm_reg_id);
                    } else {
                        englishUsersList.push(items[i].student_id);
                        englishGcmList.push(items[i].gcm_reg_id);
                    }
                }
                await Promise.all([
                    kafka.sendNotification(hindiUsersList, hindiGcmList, hindiPayload),
                    kafka.sendNotification(englishUsersList, englishGcmList, enPayload),
                ]);
            }
        }
        console.log(`the script successfully ran at ${new Date()}`);
        job.progress(100);
        return {
            data: {
                done: true,
            },
        };
    } catch (err) {
        console.log(err);
        return {
            err,
        };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "30 13 * * *", // * 19:00 at evening everyday
};
