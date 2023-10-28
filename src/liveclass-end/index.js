/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const Redis = require("ioredis");
const bluebird = require("bluebird");
const axios = require("axios");
const rp = require("request-promise");

const { config, mysql, notification } = require("../../modules");

bluebird.promisifyAll(Redis);

function getResourceByResourceReference(resourceReference) {
    const mysqlQ = `select * from (SELECT id, resource_type, resource_reference, old_detail_id, stream_status, subject FROM course_resources WHERE resource_reference = '${resourceReference}' and resource_type IN (1,4,8) limit 1) a inner join (select id, resource_reference, old_detail_id from course_resources where resource_type=2 and meta_info='Homework') as b on a.old_detail_id=b.old_detail_id`;
    console.log(mysqlQ);
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}

function getAssortmentsByResourceReference(resourceReference) {
    const mysqlQ = `select b.assortment_id from (select id from course_resources where resource_reference='${resourceReference}') as a inner join (select assortment_id, course_resource_id, resource_type  from course_resource_mapping where resource_type='resource') as b on a.id=b.course_resource_id`;
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}

function getAllParentAssortments(assortmentIDArray) {
    const mysqlQ = `select assortment_id,course_resource_id from course_resource_mapping where course_resource_id in (${assortmentIDArray}) and resource_type='assortment'`;
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}

async function getParentAssortmentListRecursively(assortmentList, totalResource = []) {
    try {
        const results = await getAllParentAssortments(assortmentList);
        if (results.length > 0) {
            const assortmentListArr = results.reduce((acc, obj) => acc.concat(obj.assortment_id), []);
            totalResource = [...totalResource, ...assortmentListArr];
            return getParentAssortmentListRecursively(assortmentListArr, totalResource);
        }
        return totalResource;
    } catch (e) {
        throw new Error(e);
    }
}

async function getParentAssortmentList(assortmentList) {
    try {
        const totalResource = [];
        const totalMapppings = await getParentAssortmentListRecursively(assortmentList, totalResource);
        return totalMapppings;
    } catch (e) {
        throw new Error(e);
    }
}

function getSubscribedUnpushedUsers(assortmentIDArray, questionID) {
    const mysqlQ = `select * from (select id, assortment_id from package where assortment_id in (${assortmentIDArray})) as a inner join (select student_id, new_package_id from student_package_subscription where start_date <= now() and end_date >= now() and is_active=1) as b on a.id=b.new_package_id inner join (select student_id, is_pushed from liveclass_subscribers where resource_reference=${questionID}) as c on b.student_id=c.student_id inner join (select student_id, gcm_reg_id, mobile, locale, app_version from students where gcm_reg_id is not null) as d on b.student_id=d.student_id`;
    console.log(mysqlQ);
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}

function upsertSubscribers(studentID, questionID) {
    const mysqlQ = `INSERT INTO liveclass_subscribers (resource_reference, student_id, is_pushed, is_interested, is_view) VALUES (${questionID}, ${studentID}, 1, 1, 1) ON DUPLICATE KEY UPDATE is_pushed = 1, is_interested=1 , is_view=1`;
    return mysql.writePool.query(mysqlQ).then(([res]) => res);
}

function getWhatsappOptinSource(mobile) {
    const mysqlQ = `select source from whatsapp_optins where phone='${mobile}'`;
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}
function subjectMap(subject) {
    const map = {
        MATHS: "गणित",
        SCIENCE: "विज्ञान",
        ENGLISH: "अंग्रेज़ी",
        "SOCIAL SCIENCE": "सामाजिक विज्ञान",
        PHYSICS: "भौतिक विज्ञान",
        CHEMISTRY: "रसायन विज्ञान",
        BIOLOGY: "जीवविज्ञान",
    };
    if (typeof map[subject] !== "undefined") {
        return map[subject];
    }
    return subject;
}
function getNotificationPayload(locale, subject, questionID, pdfUrl, type) {
    let title = "Class me rehna hai aage";
    let message = `Hello bachcho! Aaj ki ${subject} class ka homework! Class me aage rehne ke liye poora attempt karna!`;
    let imageUrl = "https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/D4A4513B-F50D-397C-A39C-309FA5E35E40.webp";
    if (locale === "hi") {
        title = "कक्षा  में रहना है आगे";
        message = `नमस्ते बच्चों ! आज की ${subjectMap(subject)} क्लास का HW लीजिये! क्लास में आगे रहने के लिए पूरा ज़रूर करना!`;
        imageUrl = "https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/8BAF2D70-EF97-195B-8E8D-19098C2755CE.webp";
    }
    if (type === "pdf") {
        return {
            event: "pdf_viewer",
            title,
            message,
            image: imageUrl,
            firebase_eventtag: `HW${questionID}`,
            data: JSON.stringify({ pdf_url: pdfUrl }),
        };
    }
    if (type === "homework_corner") {
        return {
            event: "homework",
            title,
            message,
            image: imageUrl,
            firebase_eventtag: `HW${questionID}`,
            data: JSON.stringify({ qid: questionID }),
        };
    }
}
function generateDeeplinkFromAppDeeplink(branchKey, channel, campaign, deeplink) {
    const splitted = deeplink.split("?");
    const featureSplitted = splitted[0].split("//");
    const dataSplitted = splitted[1].split("&");
    const feature = featureSplitted[1];
    const data = {};
    for (let i = 0; i < dataSplitted.length; i++) {
        const s = dataSplitted[i].split("=");
        data[s[0]] = s[1];
    }
    const myJSONObject = {
        branch_key: branchKey,
        channel,
        feature,
        campaign,
    };
    if (!_.isEmpty(data)) {
        myJSONObject.data = data;
    }
    const options = {
        url: "https://api.branch.io/v1/url",
        method: "POST",
        json: true,
        body: myJSONObject,
    };
    return rp(options);
}

function sendWhatsappPush(mobile, studentID, message, questionID, hsmData) {
    const options = {
        method: "PUT",
        url: "http://gateway.doubtnut.internal/api/whatsapp/send-text-msg",
        headers:
            { "Content-Type": "application/json" },
        body: {
            phone: `91${mobile}`,
            studentId: studentID,
            text: message,
            preview: true,
            fallbackToHSM: true,
            campaign: `HW${questionID}`,
            hsmData,
        },
        json: true,
    };
    return rp(options);
}

async function handleWhatsappPush(mobile, studentID, userLocale, questionID, subject, deeplink) {
    try {
        const sourceDetails = await getWhatsappOptinSource(mobile);
        if (sourceDetails.length > 0) {
            let message = `Dear Students ! Take the HW of today's ${subject} class ${deeplink} . Do all you can to stay ahead of the class!`;
            let templateID = 71922;
            if (userLocale === "hi") {
                subject = subjectMap(subject);
                message = `प्रिय छात्रों ! आज के ${subject} विषय का होमवर्क लें ${deeplink}! कक्षा के आगे रहने के लिए आप पूरा ज़रूर करें !`;
                templateID = 71926;
            }
            const attributes = [subject, deeplink];
            const sources = {};
            sourceDetails.map((item) => {
                if (item.source === 10) {
                    sources["8400400400"] = message;
                }
                if (item.source === 11) {
                    sources["6003008001"] = templateID;
                }
                return true;
            });
            const hsmData = {
                sources,
                attributes,
            };
            return sendWhatsappPush(mobile, studentID, message, questionID, hsmData);
        }
        return false;
    } catch (e) {
        console.log(e);
        throw new Error(e);
    }
}

async function liveclassEndAction(questionID) {
    try {
        const resourceDetails = await getResourceByResourceReference(questionID);
        if (resourceDetails.length > 0 && !_.isNull(resourceDetails[0].resource_reference)) {
            const pdfUrl = resourceDetails[0].resource_reference;
            if (!pdfUrl) {
                throw Error("Url for resource not found");
            }
            const check = await axios.get(pdfUrl);
            if (check.status === 200) {
                const assortments = await getAssortmentsByResourceReference(questionID);
                if (assortments.length > 0) {
                    let assortmentList = assortments.reduce((acc, obj) => acc.concat(obj.assortment_id), []);
                    if (assortmentList.length > 0) {
                        assortmentList = await getParentAssortmentList(assortmentList);
                        const studentDetails = await getSubscribedUnpushedUsers(assortmentList, questionID);
                        console.log(studentDetails);
                        const pdfDeeplink = await generateDeeplinkFromAppDeeplink(config.branch_key, "HOMEWORK_PDF", `HW${questionID}`, `doubtnutapp://pdf_viewer?pdf_url=${pdfUrl}`);
                        const hcDeeplink = await generateDeeplinkFromAppDeeplink(config.branch_key, "HOMEWORK_PDF", `HW${questionID}`, `doubtnutapp://homework?qid=${questionID}`);
                        for (let i = 0; i < studentDetails.length; i++) {
                            // check is pushed
                            if (_.isNull(studentDetails[i].is_pushed) || studentDetails[i].is_pushed === 0) {
                                if (!_.isNull(studentDetails[i].app_version)) {
                                    const splittedAppVersion = studentDetails[i].app_version.split(".");
                                    if (parseInt(splittedAppVersion[2]) > 188 || splittedAppVersion[0] >= 8) {
                                        // homework corner
                                        const notificationPayload = getNotificationPayload(studentDetails[i].locale, resourceDetails[0].subject, questionID, pdfUrl, "homework_corner");
                                        await notification.sendNotification([{ id: studentDetails[i].student_id, gcmId: studentDetails[i].gcm_reg_id }], notificationPayload);
                                        // await handleWhatsappPush(studentDetails[i].mobile, studentDetails[i].student_id, studentDetails[i].locale, questionID, resourceDetails[0].subject, hcDeeplink.url);
                                        await upsertSubscribers(studentDetails[i].student_id, questionID);
                                    } else {
                                        // pdf
                                        const notificationPayload = getNotificationPayload(studentDetails[i].locale, resourceDetails[0].subject, questionID, pdfUrl, "pdf");
                                        await notification.sendNotification([{ id: studentDetails[i].student_id, gcmId: studentDetails[i].gcm_reg_id }], notificationPayload);
                                        // await handleWhatsappPush(studentDetails[i].mobile, studentDetails[i].student_id, studentDetails[i].locale, questionID, resourceDetails[0].subject, pdfDeeplink.url);
                                        await upsertSubscribers(studentDetails[i].student_id, questionID);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    } catch (e) {
        console.log(e);
        throw new Error("Error in liveclass end action");
    }
}
function getFinishedLiveclass() {
    const sql = "select * from (select assortment_id, course_resource_id from course_resource_mapping where schedule_type='scheduled' and live_at > now() and live_at < date_add(now(),interval 5 minute) and resource_type='resource') as a inner join (select id, resource_reference from course_resources where resource_type=4) as b on a.course_resource_id=b.id group by b.resource_reference";
    return mysql.pool.query(sql).then(([res]) => res);
}

async function start(job) {
    try {
        const liveclasses = await getFinishedLiveclass();
        await Promise.all(liveclasses.map(async (item) => {
        // await liveclassEndAction(638675167);
            await liveclassEndAction(item.resource_reference);
        }));
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
    cron: "*/5 * * * *",
};
