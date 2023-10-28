/* eslint-disable no-await-in-loop */
const gc = require("expose-gc/function");
const moment = require("moment");
const _ = require("lodash");
const { redshift } = require("../../modules");
const { sendgridMail } = require("../../modules");
const {
    config, mysql, kafka, slack,
} = require("../../modules");

async function getTg(tgId) {
    const mysqlQ = `select * from target_group where id = '${tgId}' limit 1`;
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}

async function updateStickyNotification(id, progress) {
    const mysqlQ = `update sticky_notification set is_processed=${progress} where id=${id}`;
    return mysql.writePool.query(mysqlQ).then(([res]) => res);
}

function getTgCcmid(ccmId) {
    const mysqlQ = `select student_id,app_version,gcm_reg_id,is_online from classzoo1.students where student_id in (select student_id from classzoo1.student_course_mapping where ccm_id=${ccmId})`;
    return mysqlQ;
}

function getTgCcmidLocale(ccmId, locale) {
    const mysqlQ = `select student_id,app_version,gcm_reg_id,is_online from classzoo1.students where locale in ('${locale}') and student_id in (select student_id from classzoo1.student_course_mapping where ccm_id=${ccmId})`;
    return mysqlQ;
}

function getTgClass(studentClass) {
    const mysqlQ = `select student_id,app_version,gcm_reg_id,is_online from classzoo1.students where student_class=${studentClass}`;
    return mysqlQ;
}

function getTgClassLocale(studentClass, locale) {
    const mysqlQ = `select student_id,app_version,gcm_reg_id,is_online from classzoo1.students where student_class=${studentClass} and locale in ('${locale}')`;
    return mysqlQ;
}

async function getDefaultVariantFromAssortmentIdHome(assortmentId) {
    const mysqlQ = `select * from (select id, type, assortment_id, name, description, duration_in_days from package where assortment_id=${assortmentId} and flag_key is null and reference_type='v3' and type='subscription') as a inner join (select id as variant_id, package_id, base_price, display_price from variants where is_default=1) as b on a.id=b.package_id order by a.duration_in_days`;
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}

async function getMandatoryStudentIds() {
    const mysqlQ = "select student_id,app_version,gcm_reg_id,is_online from students where student_id in (61159710,72662372,65560884,54050781,24593113,3215112,3258590)";
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}

async function checkStickyCurrentStatusForConcurrency(id) {
    const mysqlQ = `select is_processed from sticky_notification where id=${id}`;
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}

async function getActiveBoard() {
    const mysqlQ = "select * from class_course_mapping where is_active=1 and category = 'board'";
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}

async function getActiveExam() {
    const mysqlQ = "select * from class_course_mapping where is_active=1 and category = 'exam'";
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}

async function getCourseByCCMIdHindi(item) {
    const mysqlQ = `select * from course_details cd inner join (select image_url as image_thumbnail, assortment_id as course_thumbnail_assortment_id from course_details_thumbnails where is_active = 1 and class = ${item.class} and type = 'widget_popular_course') as b on cd.assortment_id = b.course_thumbnail_assortment_id where cd.category="${item.category}" and cd.meta_info in ('HINGLISH', 'HINDI') and cd.is_free = 0 and cd.is_active = 1 and cd.is_active_sales = 1 and cd.class = ${item.class} and cd.assortment_type = 'course' order by cd.priority asc limit 1`;
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}

async function getCourseByCCMIdEnglish(item) {
    const mysqlQ = `select * from course_details cd inner join (select image_url as image_thumbnail, assortment_id as course_thumbnail_assortment_id from course_details_thumbnails where is_active = 1 and class = ${item.class} and type = 'widget_popular_course') as b on cd.assortment_id = b.course_thumbnail_assortment_id where cd.category="${item.category}" and cd.meta_info in ('HINGLISH', 'ENGLISH') and cd.is_free = 0 and cd.is_active = 1 and cd.is_active_sales = 1 and cd.class = ${item.class} and cd.assortment_type = 'course' order by cd.priority asc limit 1`;
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

async function getAllCourseByCCMIds(distinctCCMIdArray) {
    try {
        const examCategoryMapping = {
            ACT: null,
            "Andhra Pradesh Board": "State Boards",
            "BANK PO": "Govt. Exams",
            "BANKING AND INSURANCE": "Govt. Exams",
            "Bihar Board": "Bihar Board",
            BITSAT: "IIT JEE",
            "BOARD EXAMS": "CBSE Boards",
            CBSE: "CBSE Boards",
            "Chhattisgarh Board": "Chhattisgarh Board",
            DEFENCE: "NAVY SSR & AA",
            SSC: "SSC GD",
            "Delhi Board": "State Boards",
            DU: "Govt. Exams",
            ENGINEERING: "IIT JEE",
            FOUNDATION: "IIT JEE",
            "Civil Services": "Civil Services",
            "Gujarat Board": "Gujarat Board",
            "Haryana Board": "Haryana Board",
            "Himachal Pradesh Board": "Himachal Board",
            ICSE: "State Boards",
            "IIT JEE": "IIT JEE",
            "Jharkhand Board": "Jharkhand Board",
            JNU: "State Boards",
            "Karnataka Board": "State Boards",
            "Kerala Board": "State Boards",
            KVPY: "IIT JEE",
            "Madhya Pradesh Board": "MP Board",
            "Maharashtra Board": "Maharashtra Board",
            NDA: "NDA",
            NEET: "NEET",
            "Nepal Board": "State Boards",
            NTSE: "IIT JEE",
            "Odisha Board": "State Boards",
            "OTHER EXAM": "For All",
            "Other State Board": "State Boards",
            "Punjab Board": "State Boards",
            RAILWAYS: "Railways",
            "Rajasthan Board": "Rajasthan Board",
            SAT: null,
            "SCHOOL/BOARD EXAM": "CBSE Boards",
            "SSC CGL": "Govt. Exams",
            "Tamil Nadu Board": "State Boards",
            TEACHING: "Teaching",
            "Telangana Board": "State Boards",
            "UP Board": "UP Board",
            UPSC: "Govt. Exams",
            "Uttarakhand Board": "Uttarakhand Board",
            VITEEE: "IIT JEE",
            WBJEE: "IIT JEE",
            "West Bengal Board": "State Boards",
            "State Police": "State Police",
            IT: "IT",
        };
        distinctCCMIdArray.forEach((item) => { item.category = examCategoryMapping[item.course]; });
        const promisesHi = [];
        const promisesEn = [];
        for (let i = 0; i < distinctCCMIdArray.length; i++) {
            promisesHi.push(getCourseByCCMIdHindi(distinctCCMIdArray[i]));
            promisesEn.push(getCourseByCCMIdEnglish(distinctCCMIdArray[i]));
        }
        let resultHindi = await Promise.all(promisesHi);
        for (let i = 0; i < resultHindi.length; i++) {
            if (resultHindi[i].length > 0) {
                resultHindi[i][0].ccm_id = distinctCCMIdArray[i].id;
                resultHindi[i][0].locale = "hi";
            }
        }
        let resultEnglish = await Promise.all(promisesEn);
        for (let i = 0; i < resultEnglish.length; i++) {
            if (resultEnglish[i].length > 0) {
                resultEnglish[i][0].ccm_id = distinctCCMIdArray[i].id;
                resultEnglish[i][0].locale = "en";
            }
        }
        resultHindi = _.flatten(resultHindi);
        resultEnglish = _.flatten(resultEnglish);
        const final = _.concat(resultHindi, resultEnglish);
        const worker1 = [];
        for (let i = 0; i < final.length; i++) {
            worker1.push(generateAssortmentVariantMapping(final[i].assortment_id));
        }
        const result = await Promise.all(worker1);
        const finalResult = [];
        for (let i = 0; i < final.length; i++) {
            if (result[i][final[i].assortment_id] && result[i][final[i].assortment_id].display_price != 0) {
                final[i].package_variant = result[i][final[i].assortment_id].package_variant;
                final[i].base_price = result[i][final[i].assortment_id].base_price;
                final[i].display_price = result[i][final[i].assortment_id].display_price;
                final[i].duration = result[i][final[i].assortment_id].duration;
                finalResult.push(final[i]);
            }
        }
        return finalResult;
    } catch (error) {
        console.log(error);
        return [];
    }
}

function getNotificationPayload(notificationData, assortmentData, payloadType, loopNumber) {
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
    } else if (payloadType === "timer") {
        const time = moment(notificationData.timer_end_time).format();
        const final = new Date(time).toISOString();
        notifdata = {
            id: notificationData.id,
            image_url: notificationData.image_url,
            deeplink_banner: notificationData.deeplink_banner,
            end_time: final,
            text: notificationData.text_under_price,
            text_color: notificationData.text_color,
            timer_text_color: notificationData.text_color,
            cta: notificationData.button_cta,
            cta_text_color: notificationData.text_color,
            cta_text_size: "19",
            background_color: notificationData.bg_color,
            cta_background_color: notificationData.cta_bg_color,
        };
    } else if ([4, 5, 6, 7].includes(notificationData.is_campaign)) {
        notifdata = {
            id: notificationData.id,
            image_url: assortmentData.image_thumbnail,
            is_vanish: notificationData.is_vanish !== "0",
            price: `₹ ${assortmentData.display_price}`,
            price_color: "#efefef",
            crossed_price: `₹ ${assortmentData.base_price}`,
            crossed_price_color: "#d9d7d7",
            cross_color: "#d9d7d7",
            text: notificationData.text_under_price,
            text_color: "#efefef",
            button_cta: notificationData.button_cta,
            button_text_color: "#ffffff",
            button_background_color: "#ea532c",
            deeplink_banner: notificationData.deeplink_banner ? `doubtnutapp://course_details?id=${assortmentData.assortment_id}||||${notificationData.deeplink_banner}` : `doubtnutapp://course_details?id=${assortmentData.assortment_id}`,
            deeplink_button: notificationData.deeplink_button ? `doubtnutapp://course_details?id=${assortmentData.assortment_id}||||${notificationData.deeplink_button}` : `doubtnutapp://course_details?id=${assortmentData.assortment_id}`,
            offset: +notificationData.offset * 1000,
        };
        if ([5, 7].includes(notificationData.is_campaign)) {
            notifdata.deeplink_button = notificationData.deeplink_button ? `doubtnutapp://vip?variant_id=${assortmentData.package_variant}&coupon_code=${notificationData.deeplink_button}` : `doubtnutapp://vip?variant_id=${assortmentData.package_variant}`;
        }
    } else {
        notifdata = {
            id: notificationData.id,
            image_url: notificationData.image_url,
            is_vanish: notificationData.is_vanish !== "0",
            price: `₹ ${assortmentData[notificationData.assortment_id].display_price}`,
            price_color: "#efefef",
            crossed_price: `₹ ${assortmentData[notificationData.assortment_id].base_price}`,
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
    const notifdataFinal = {
        s_n_id: `sticky-${notificationData.id}-${loopNumber}-${payloadType}-${date}`,
        title: (payloadType === "text") ? notificationData.title_text_notification : notificationData.title_image_notification,
        message: (payloadType === "text") ? notificationData.message_text_notification : notificationData.message_image_notification,
        data: notifdata,
    };
    if (payloadType === "timer") {
        notifdataFinal.event = "generic_sticky_timer";
        notifdataFinal.firebase_eventtag = "sticky_timer_notification";
        notifdataFinal.image_url = notificationData.image_url;
    } else {
        notifdataFinal.event = "course_notification";
        notifdataFinal.firebase_eventtag = `sticky_${notificationData.id}`;
        notifdataFinal.sn_type = payloadType;
    }
    return notifdataFinal;
}

async function notificationNotProcessedAction(notificationData, mandatoryStudents) {
    const fromEmail = "autobot@doubtnut.com";
    const toEmail = "saurabh.raj@doubtnut.com";
    const ccList = ["dipankar@doubtnut.com", "aditya.mishra@doubtnut.com", "yash.bansal@doubtnut.com", "rohan@doubtnut.com", "gaurav@doubtnut.com", "aashish.jaiswal@doubtnut.com"];
    const blockNew = [];
    try {
        const checkCurrentStatus = await checkStickyCurrentStatusForConcurrency(notificationData.id);
        if (!_.isEmpty(checkCurrentStatus) && checkCurrentStatus[0].is_processed == 0) {
            await updateStickyNotification(notificationData.id, -1);
            const targetGroupInfo = await getTg(notificationData.target_group_id);
            let query;
            if (targetGroupInfo[0].sql != null) {
                if (targetGroupInfo[0].sql[targetGroupInfo[0].sql.length - 1] === ";") {
                    targetGroupInfo[0].sql = targetGroupInfo[0].sql.slice(0, targetGroupInfo[0].sql.length - 1);
                }
                query = targetGroupInfo[0].sql.replace(/\n/g, " ").replace(/\r/g, "");
            } else if (targetGroupInfo[0].user_exam) {
                query = (targetGroupInfo[0].user_locale === null) ? getTgCcmid(targetGroupInfo[0].user_exam) : getTgCcmidLocale(targetGroupInfo[0].user_exam, targetGroupInfo[0].user_locale);
            } else if (targetGroupInfo[0].user_class) {
                query = (targetGroupInfo[0].user_locale === null) ? getTgClass(targetGroupInfo[0].user_class) : getTgClassLocale(targetGroupInfo[0].user_class, targetGroupInfo[0].user_locale);
            }
            const count = [0, 0, 0];
            for (let a = 0; a < 3; a++) {
                let l = 1;
                const now = moment().add(5, "hour").add(30, "minutes").format();
                console.log(`Starting Notification Number :- ${notificationData.id} and Loop Number :- ${a + 1} at time :- ${now}`);
                // eslint-disable-next-line no-constant-condition
                while (true) {
                // calling garbage collector for every loop
                    gc();
                    const now2 = moment().add(5, "hour").add(30, "minutes").format();
                    console.log(`Data fetch start of chunk number :- ${l} for Notification Number :- ${notificationData.id} and Loop Number :- ${a + 1} at time :- ${now2}`);
                    let sql;
                    if ([4, 5, 6, 7].includes(notificationData.is_campaign)) {
                        sql = `select student_id,gcm_reg_id,app_version,is_online,locale from (${query}) tg_result ORDER BY tg_result.student_id DESC LIMIT 50000 OFFSET ${(l - 1) * 50000}`;
                    } else {
                        sql = `select student_id,gcm_reg_id,app_version,is_online from (${query}) tg_result ORDER BY tg_result.student_id DESC LIMIT 50000 OFFSET ${(l - 1) * 50000}`;
                    }
                    let studentDetails = await redshift.query(sql);
                    if (studentDetails.length === 0) {
                        break;
                    }
                    const now3 = moment().add(5, "hour").add(30, "minutes").format();
                    console.log(`Data fetch end of chunk number :- ${l} for Notification Number :- ${notificationData.id} and Loop Number :- ${a + 1} at time :- ${now3}`);
                    if (l === 1 && ![4, 5, 6, 7].includes(notificationData.is_campaign)) {
                        studentDetails = mandatoryStudents.concat(studentDetails);
                    }
                    const usersImageStudentId = [];
                    const usersImageGcm = [];
                    const userCampaignStudentId = [];
                    const userCampaignGcm = [];
                    const usersTextStudentId = [];
                    const usersTextGcm = [];
                    const userNotInstalledGCM = [];
                    const usersTimerStudentId = [];
                    const usersTimerGcm = [];
                    let studentTrueCount = 0;
                    studentDetails = studentDetails.filter((item) => item.gcm_reg_id !== null);
                    if (notificationData.is_campaign === 8) {
                        for (let i = 0; i < studentDetails.length; i++) {
                            userNotInstalledGCM.push(studentDetails[i].gcm_id);
                        }
                    } else {
                        for (let i = 0; i < studentDetails.length; i++) {
                            if (studentDetails[i] && studentDetails[i].app_version && ![4, 5, 6, 7].includes(notificationData.is_campaign)) {
                                const appVersion = studentDetails[i].is_online;
                                if (appVersion >= 878) {
                                    if (notificationData.is_campaign === 1) {
                                        userCampaignStudentId.push(studentDetails[i].student_id);
                                        userCampaignGcm.push(studentDetails[i].gcm_reg_id);
                                        ++studentTrueCount;
                                    } else if (notificationData.is_campaign === 2) {
                                        usersTextStudentId.push(studentDetails[i].student_id);
                                        usersTextGcm.push(studentDetails[i].gcm_reg_id);
                                        ++studentTrueCount;
                                    } else if (notificationData.is_campaign === 9) {
                                        usersTimerStudentId.push(studentDetails[i].student_id);
                                        usersTimerGcm.push(studentDetails[i].gcm_reg_id);
                                        ++studentTrueCount;
                                    } else {
                                        usersImageStudentId.push(studentDetails[i].student_id);
                                        usersImageGcm.push(studentDetails[i].gcm_reg_id);
                                        ++studentTrueCount;
                                    }
                                }
                            }
                        }
                    }
                    const assortmentData = await generateAssortmentVariantMapping(notificationData.assortment_id);
                    let payloadImage;
                    let payloadText;
                    let payloadCampaign;
                    let payloadTimer;
                    const now4 = moment().add(5, "hour").add(30, "minutes").format();
                    console.log(`Notification send start of chunk number :- ${l} for Notification Number :- ${notificationData.id} and Loop Number :- ${a + 1} at time :- ${now4}`);
                    if (usersImageStudentId.length !== 0 && usersImageGcm.length !== 0) {
                        const payloadType = "image";
                        payloadImage = getNotificationPayload(notificationData, assortmentData, payloadType, a);
                    }
                    if (usersTextStudentId.length !== 0 && usersTextGcm.length !== 0) {
                        const payloadType = "text";
                        payloadText = getNotificationPayload(notificationData, assortmentData, payloadType, a);
                    }
                    if (userCampaignStudentId.length !== 0 && userCampaignGcm.length !== 0) {
                        const payloadType = "banner";
                        payloadCampaign = getNotificationPayload(notificationData, assortmentData, payloadType, a);
                    }
                    if ([4, 5, 6, 7].includes(notificationData.is_campaign) && studentDetails.length > 0) {
                        const payloadType = "image";
                        let getActiveCCM = [];
                        if ([4, 5].includes(notificationData.is_campaign)) {
                            getActiveCCM = await getActiveBoard();
                            const mapping = [{
                                id: 601,
                                course: "CBSE",
                                class: 6,
                            }, {
                                id: 701,
                                course: "CBSE",
                                class: 7,
                            }, {
                                id: 801,
                                course: "CBSE",
                                class: 8,
                            }];
                            getActiveCCM.push(...mapping);
                            _.forEach(studentDetails, (item) => {
                                if (item.ccm_id === null && ["6", "7", "8", 6, 7, 8].includes(item.student_class)) {
                                    const index = _.findIndex(mapping, (o) => o.class == item.student_class);
                                    if (index !== -1) {
                                        item.ccm_id = mapping[index].id;
                                    }
                                }
                            });
                        } else if ([6, 7].includes(notificationData.is_campaign)) {
                            getActiveCCM = await getActiveExam();
                        }
                        const getActiveBoardsArr = getActiveCCM.map((item) => item.id);
                        const finalStudents = studentDetails.filter((item) => getActiveBoardsArr.includes(item.ccm_id));
                        const distinctCCMId = _.uniqBy(finalStudents, "ccm_id");
                        const distinctCCMIdArray = distinctCCMId.map((item) => item.ccm_id);
                        const ccmList = getActiveCCM.filter((item) => distinctCCMIdArray.includes(item.id));
                        const finalCourses = await getAllCourseByCCMIds(ccmList);
                        finalStudents.forEach((item) => {
                            if (item.locale !== "hi") {
                                item.locale = "en";
                            }
                        });
                        const groupByCCM = _.groupBy(finalCourses, "ccm_id");
                        for (const key in groupByCCM) {
                            if (groupByCCM[key].length) {
                                const ccmCourses = groupByCCM[key];
                                const groupByLocale = _.groupBy(ccmCourses, "locale");
                                for (const key2 in groupByLocale) {
                                    if (groupByLocale[key2].length) {
                                        const ccmCoursesLocale = groupByLocale[key2];
                                        const payload = getNotificationPayload(notificationData, ccmCoursesLocale[0], payloadType, a);
                                        const tempStudentList = finalStudents.filter((item) => item.ccm_id == key && item.locale == key2);
                                        const studentList = tempStudentList.map((item) => item.student_id);
                                        const gcmList = tempStudentList.map((item) => item.gcm_reg_id);
                                        if (studentList.length !== 0 && gcmList.length !== 0) {
                                            await kafka.sendNotification(studentList, gcmList, payload);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (userNotInstalledGCM.length !== 0) {
                        let payloadType = "banner";
                        if (notificationData.type_extra === "image") {
                            payloadType = "image";
                        } else if (notificationData.type_extra === "text") {
                            payloadType = "text";
                        }
                        const payload = getNotificationPayload(notificationData, assortmentData, payloadType, a);
                        await kafka.sendNotificationWithoutStudentIds([], userNotInstalledGCM, payload);
                    }
                    if (usersTimerStudentId.length !== 0 && usersTimerGcm.length !== 0) {
                        const payloadType = "timer";
                        payloadTimer = getNotificationPayload(notificationData, assortmentData, payloadType, a);
                    }
                    if (usersImageStudentId.length > 0 && usersImageGcm.length > 0) {
                        await kafka.sendNotification(usersImageStudentId, usersImageGcm, payloadImage);
                    }
                    if (userCampaignStudentId.length > 0 && userCampaignGcm.length > 0) {
                        await kafka.sendNotification(userCampaignStudentId, userCampaignGcm, payloadCampaign);
                    }
                    if (usersTextStudentId.length > 0 && usersTextGcm.length > 0) {
                        await kafka.sendNotification(usersTextStudentId, usersTextGcm, payloadText);
                    }
                    if (usersTimerStudentId.length > 0 && usersTimerGcm.length > 0) {
                        await kafka.sendNotification(usersTimerStudentId, usersTimerGcm, payloadTimer);
                    }
                    const now5 = moment().add(5, "hour").add(30, "minutes").format();
                    console.log(`Notification send end of chunk number :- ${l} for Notification Number :- ${notificationData.id} and Loop Number :- ${a + 1} at time :- ${now5}`);
                    l++;
                    count[a] += studentTrueCount;
                }
            }
            await updateStickyNotification(notificationData.id, 1);
            try {
                const body = `The TG_id is ${notificationData.target_group_id} and the count of notfication sent for students in each loop are:- \nLoop 1- ${count[0]} ; \nLoop 2:- ${count[1]}; \nLoop 3:- ${count[2]}`;
                const bodySlack = `The TG_id is ${notificationData.target_group_id} for Notification Id ${notificationData.id} and the count of notfication sent for students in each loop are:- \nLoop 1- ${count[0]} ; \nLoop 2:- ${count[1]} \nLoop 3:- ${count[2]}`;
                blockNew.push({
                    type: "section",
                    text: { type: "mrkdwn", text: bodySlack },
                });
                await slack.sendMessage("#sticky-notification-delivery", blockNew, config.STICKY_SLACK_AUTH);
                sendgridMail.sendMail(fromEmail, toEmail, `Sticky Notification sent :- ${notificationData.id}`, body, [], ccList);
            } catch (e) {
                console.log(e);
                blockNew.push({
                    type: "section",
                    text: { type: "mrkdwn", text: `Error in sticky notification mails <@U01MGHVFE4U> <@ULGN432HL> <@U01J94LFQTW> <@UFQDL9UMS> <@U01N7BD6Y6N> <U02P3P0LSUE> :- ${e.message}` },
                });
                await slack.sendMessage("#sticky-notification-delivery", blockNew, config.STICKY_SLACK_AUTH);
                await sendgridMail.sendMail(fromEmail, toEmail, "CRON | ALERT!!! Sticky Notification Mail Error ", e.message, [], ccList);
            }
        }
    } catch (e) {
        console.log(e);
        blockNew.push({
            type: "section",
            text: { type: "mrkdwn", text: `Error in sticky notification <@U01MGHVFE4U> <@U01J94LFQTW> <@UFQDL9UMS> <@U01N7BD6Y6N> <@U02P3P0LSUE> :- ${e.message}` },
        });
        await slack.sendMessage("#sticky-notification-delivery", blockNew, config.STICKY_SLACK_AUTH);
        await sendgridMail.sendMail(fromEmail, toEmail, "CRON | ALERT!!! Sticky Notification Script Error ", e.message, [], ccList);
        throw new Error("Error in sticky notification");
    }
}

async function getStickyNotification() {
    const mysqlQ = "select * from sticky_notification where is_processed=0 and start_date < now() and end_date > now()";
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}

async function start(job) {
    try {
        const stickyNotificationData = await getStickyNotification();
        const mandatoryStudents = await getMandatoryStudentIds();
        for (let i = 0; i < stickyNotificationData.length; i++) {
            await notificationNotProcessedAction(stickyNotificationData[i], mandatoryStudents);
        }
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
    cron: "13,43 * * * *",
};
