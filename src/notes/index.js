/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const moment = require("moment");
const axios = require("axios");
const {
    mysql, kafka,
} = require("../../modules");

async function getNotesAssortment() {
    const sql = `select * from (select id, resource_reference, chapter from course_resources where created_at >='${moment().format("YYYY-MM-DD")}  00:00:00' and created_at < '${moment().add(1, "days").format("YYYY-MM-DD")}  00:00:00' and resource_type=2 and meta_info like 'notes') as a left join (select assortment_id, course_resource_id from course_resource_mapping where resource_type='resource') as b on a.id=b.course_resource_id`;
    console.log(sql);
    return mysql.pool.query(sql).then((res) => res[0]);
}

function getAllParentAssortments(assortmentIDArray) {
    const sql = `select assortment_id,course_resource_id from course_resource_mapping where course_resource_id in (${assortmentIDArray.join(",")}) and resource_type='assortment'`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

function getSubscribedUsersByAssortmentList(assortmentIDArray) {
    const sql = `select b.student_id, c.gcm_reg_id, c.mobile, c.locale from (select * from package where assortment_id in (${assortmentIDArray}))  as a inner join (select student_id, new_package_id from student_package_subscription where start_date < now() and end_date > now() and is_active=1) as b on a.id=b.new_package_id left join (select * from students where gcm_reg_id is not null) as c on b.student_id=c.student_id group by b.student_id`;
    return mysql.pool.query(sql).then((res) => res[0]);
}
async function getParentAssortmentListRecursivelyV1(assortmentList, totalResource = []) {
    try {
        const results = await getAllParentAssortments(assortmentList);
        if (results.length > 0) {
            totalResource = [...totalResource, ...results];
            const assortmentListArr = results.reduce((acc, obj) => acc.concat(obj.assortment_id), []);
            return getParentAssortmentListRecursivelyV1(assortmentListArr, totalResource);
        }
        return totalResource;
    } catch (e) {
        throw new Error(e);
    }
}
async function getParentAssortmentListV1(assortmentList) {
    try {
        const totalResource = [];
        const totalMapppings = await getParentAssortmentListRecursivelyV1(assortmentList, totalResource);
        // divide it into resources and assortment ids
        return totalMapppings;
    } catch (e) {
        throw new Error(e);
    }
}

function getNotificationPayload(locale, chapterName, pdfUrl, id) {
    let title = `${chapterName} ke notes`;
    let message = "View now !";
    let imageUrl = "https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/5AD84CB1-2B9B-5A86-0F48-40F050DAC490.webp";
    if (locale === "hi") {
        title = `${chapterName} के नोट्स`;
        message = "अभी देखें !";
        imageUrl = "https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/ABC117CF-CA6E-5ADE-D925-F7BC8EE1975E.webp";
    }
    return {
        event: "pdf_viewer",
        title,
        message,
        image: imageUrl,
        firebase_eventtag: `NOTES_${id}`,
        data: JSON.stringify({ pdf_url: pdfUrl }),
    };
}

async function start(job) {
    try {
        job.progress(0);
        const result = await getNotesAssortment();

        // group by assortment id
        const groupedAssortments = _.groupBy(result, "assortment_id");
        for (const key in groupedAssortments) {
            if (Object.prototype.hasOwnProperty.call(groupedAssortments, key)) {
                const item = groupedAssortments[key];
                const check = await axios.get(item[0].resource_reference);
                if (check.status === 200) {
                // console.log(item)
                    const assortmentID = key;
                    // get all parent assortments
                    const assList = await getParentAssortmentListV1([assortmentID]);
                    const list = assList.reduce((acc, obj) => acc.concat(obj.assortment_id), [parseInt(assortmentID)]);
                    // get active subscribed users
                    const users = await getSubscribedUsersByAssortmentList(list);
                    // console.log(users)
                    const hindiPayload = getNotificationPayload("hi", item[0].chapter, item[0].resource_reference, item[0].id);
                    const enPayload = getNotificationPayload("en", item[0].chapter, item[0].resource_reference, item[0].id);
                    const hindiUsersList = [];
                    const hindiGcmList = [];
                    const englishUsersList = [];
                    const englishGcmList = [];
                    for (let i = 0; i < users.length; i++) {
                        if (users[i].locale === "hi") {
                            hindiUsersList.push(users[i].student_id);
                            hindiGcmList.push(users[i].gcm_reg_id);
                        } else {
                            englishUsersList.push(users[i].student_id);
                            englishGcmList.push(users[i].gcm_reg_id);
                        }
                    }
                    await Promise.all([
                        kafka.sendNotification(hindiUsersList, hindiGcmList, hindiPayload),
                        kafka.sendNotification(englishUsersList, englishGcmList, enPayload),
                    ]);
                }
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
    cron: "30 9 * * *", // * 15:00 at afternoon everyday
};
