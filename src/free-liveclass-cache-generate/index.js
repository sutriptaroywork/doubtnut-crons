/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const redisClient = require("../../modules/redis");
const { redshift, mysql, config } = require("../../modules");

const subjectImage = {
    maths: `${config.cdn_url}engagement_framework/F15A73FA-97F2-C673-F928-E8CDABCB61D6.webp`,
    physics: `${config.cdn_url}engagement_framework/4BEE5CA2-A42F-B9D1-8E2B-5EC5636BDE99.webp`,
    biology: `${config.cdn_url}engagement_framework/C566F597-DFE5-3F06-23A8-3147A7BA1E76.webp`,
    chemistry: `${config.cdn_url}engagement_framework/989C60D2-A754-0315-FE9E-FD94CA0245FF.webp`,
    science: `${config.cdn_url}engagement_framework/D8D4F256-468A-81A7-8140-FA064264B513.webp`,
    english: `${config.cdn_url}engagement_framework/AF3C1C55-252C-267C-5BF9-EF1961FD1AF4.webp`,
    "social science": `${config.cdn_url}engagement_framework/27325004-AD29-B931-2DA9-1AC3111A44FC.webp`,
    "english grammar": `${config.cdn_url}engagement_framework/AF3C1C55-252C-267C-5BF9-EF1961FD1AF4.webp`,
    guidance: `${config.cdn_url}engagement_framework/5395C12D-97AB-F256-6341-AC0FA3B5FEEF.webp`,
    default: `${config.cdn_url}engagement_framework/90369CAC-985B-B900-9D37-CA332EAEDF62.webp`,
};

const subjetBgImageCard = {
    maths: `${config.staticCDN}engagement_framework/52F7EFED-8C96-6323-4FE5-EFD826A055B7.webp`,
    physics: `${config.staticCDN}engagement_framework/8ECF24CB-EDF6-2676-ADC1-7A4EEFA0C66D.webp`,
    biology: `${config.staticCDN}engagement_framework/C93D8678-33BF-D433-0EA6-507BF388A5FE.webp`,
    chemistry: `${config.staticCDN}engagement_framework/7252A2B4-6A58-4D0F-2C35-C9C8847E6DBC.webp`,
    science: `${config.staticCDN}engagement_framework/EF34D550-D283-4007-2876-463C03D98DFF.webp`,
    english: `${config.staticCDN}engagement_framework/215F37C9-FC92-521B-2025-6F56AF81218B.webp`,
    economics: `${config.staticCDN}engagement_framework/C772ECF1-30C4-02FE-43B9-10EE7558F6AF.webp`,
    default: `${config.staticCDN}engagement_framework/93EBAD2A-CE29-4E29-515E-DD80DC5D9410.webp`,
    political_science: `${config.staticCDN}engagement_framework/AB8A4C78-941F-9065-29F8-C8811648B9C0.webp`,
    reasoning: `${config.staticCDN}engagement_framework/A274D354-8233-9888-2DE5-75E6966269C7.webp`,
    history: `${config.staticCDN}engagement_framework/6F6A6281-13D4-5D35-7A8E-FE5EF0FC1999.webp`,
    geography: `${config.staticCDN}engagement_framework/628EA1FB-0FC9-DAE0-3585-D8B1F4ECAC52.webp`,
    "social science": `${config.staticCDN}engagement_framework/2F6A103B-7E13-7DA5-BF2E-8C49F7FB25C0.webp`,
    "english grammar": `${config.staticCDN}engagement_framework/F50D3DAA-895C-4C4B-D00D-90BF1F911307.webp`,
};

function getLiveClassVideo(sClass, language) {
    const sql = `select sum(a.engage_time) as count, c.assortment_id, d.class, d.meta_info,b.resource_reference, b.subject,b.display, 
    b.expert_name,b.topic,case when du.image_url_left_full is null then du.image_url ELSE du.image_url_left_full end as expert_image from (select question_id,engage_time from classzoo1.video_view_stats where source='android' and engage_time<>0 and created_at + interval '330 minutes'>= (CURRENT_DATE - INTERVAL '1 DAY')) as a inner join classzoo1.course_resources as b on a.question_id=b.resource_reference inner join classzoo1.dashboard_users as du on b.faculty_id = du.id INNER join classzoo1.course_resource_mapping as c on b.id=c.course_resource_id INNER join classzoo1.course_details as d on c.assortment_id=d.assortment_id WHERE b.resource_type in (1,4,8) and c.resource_type='resource' and d.is_free=1 and d.class=${sClass} and d.meta_info='${language}' and b.subject not in ('ENGLISH GRAMMAR') GROUP by b.resource_reference, c.assortment_id, d.class, d.meta_info, b.subject,b.display,b.expert_name,b.expert_image,b.topic,du.image_url_left_full,du.image_url ORDER by  sum(a.engage_time) desc limit 10`;
    // console.log(sql);
    return redshift.query(sql).then((res) => res);
}

function getLiveClassChapterData(sClass, language) {
    const sql = `select count(b.topic) as count, c.assortment_id, e.class,e.meta_info, b.resource_reference, b.subject,b.display,b.expert_name,b.expert_image,b.topic, d.assortment_id as chapter_assortment_id from (select question_id from classzoo1.video_view_stats where source='android' and engage_time<>0 and created_at + interval '330 minutes'>= (CURRENT_DATE - INTERVAL '1 DAY')) as a inner join classzoo1.course_resources as b on a.question_id=b.resource_reference INNER join classzoo1.course_resource_mapping as c on b.id=c.course_resource_id INNER JOIN classzoo1.course_resource_mapping as d on c.assortment_id=d.course_resource_id INNER join classzoo1.course_details as e on d.assortment_id=e.assortment_id WHERE b.resource_type in (1,4,8) and c.resource_type='resource' and d.resource_type='assortment' and e.is_free=1 and e.class=${sClass} and e.meta_info='${language}' GROUP by b.topic, c.assortment_id, e.class,e.meta_info, b.resource_reference, b.subject,b.display,b.expert_name,b.expert_image,b.topic, d.assortment_id ORDER by count(b.topic) desc limit 10`;
    // console.log("topic\n", sql);
    return redshift.query(sql).then((res) => res);
}

function getTeachersData(sClass, language) {
    const sql = `SELECT lc.class, lc.locale, lc.course_exam, lcr.expert_name, lcd.faculty_id, b.faculty_name,b.image_url,b.subject_name_localised, b.college,b.rating,b.experience_in_hours,b.students_mentored,c.priority, count(distinct resource_reference) FROM liveclass_course_details lcd left join liveclass_course_resources lcr on lcr. liveclass_course_detail_id = lcd.id left join liveclass_course lc on lc.id = lcr.liveclass_course_id left join course_details_liveclass_course_mapping cd on cd. liveclass_course_id = lc.id left join course_teacher_mapping b on lcd.faculty_id =  b.faculty_id and cd.assortment_id = b.assortment_id left join (select class , locale , case when category = 'IIT JEE' then 'IIT' else category end as category, faculty_id , priority from categorywise_faculty_ordering )c on c.class =  lc.class and c.locale =  lc.locale and c.category =  lc.course_exam and c.faculty_id = lcd.faculty_id WHERE live_at >= date_sub(current_date, INTERVAL 6 DAY) and live_at <= date_sub(current_date,INTERVAL 1 DAY) and lcr.resource_type in (4) and lcd.is_replay = 0 and lcr.subject not in ('ALL','ANNOUNCEMENT','GUIDANCE') and lc.is_free = 1 and lc.class = ${sClass}  and b.is_free = 1 and lc.locale = '${language}' group by 5 order by 13 asc limit 10`;
    // console.log("teacher\n", sql);
    return mysql.pool.query(sql).then(([res]) => res);
}

async function createRecommendedPlaylist(data, sClass, language) {
    if (data && data.length) {
        console.log("recommended data length", data.length);
        const qidKeys = {};
        const finalData = [];
        for (let k = 0; k < data.length; k++) {
            if (data[k].resource_reference && !qidKeys[data[k].resource_reference] && Object.keys(qidKeys).length < 5) {
                const obj = {
                    type: "widget_child_autoplay",
                    data: {
                        id: `${data[k].resource_reference}`,
                        assortment_id: data[k].assortment_id,
                        page: "LIVECLASS_FREE",
                        title1: `${_.startCase(data[k].subject.toLowerCase())}, Class ${data[k].class}`,
                        title2: `${data[k].display}`,
                        image_url: `${data[k].expert_image}`,
                        image_bg_card: `${subjetBgImageCard[data[k].subject.toLowerCase()] || subjetBgImageCard.default}`,
                        subject: null,
                        color: "#ccddff",
                        player_type: "liveclass",
                        live_at: 1640031300000,
                        lock_state: 0,
                        bottom_title: "",
                        topic: "",
                        students: 13822,
                        interested: 13822,
                        is_premium: false,
                        state: 2,
                        show_reminder: false,
                        reminder_message: "Reminder has been set",
                        payment_deeplink: `doubtnutapp://vip?assortment_id=${data[k].assortment_id}`,
                        card_width: "1.25",
                        card_ratio: "16:9",
                        text_color_primary: "#ffffff",
                        text_color_secondary: "#ffffff",
                        text_color_title: "#ffffff",
                        set_width: true,
                        button_state: "multiple",
                        image_vertical_bias: 1,
                        bg_exam_tag: "#622abd",
                        text_color_exam_tag: "#ffffff",
                        target_exam: "",
                        button: {
                            text: "Go to Chapter",
                            deeplink: "",
                        },
                        deeplink: "",
                        bottom_layout: {
                            title: `Class ${data[k].class}, ${data[k].topic}`,
                            title_color: "#504e4e",
                            sub_title: `${data[k].count} attended`,
                            sub_title_color: "#5b5b5b",
                            button: {
                                text: "",
                                text_color: "#ea532c",
                                background_color: "#00000000",
                                border_color: "#ea532c",
                                deeplink: "",
                                text_all_caps: false,
                                show_volume: false,
                            },
                            icon_subtitle: "https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/509EE326-9771-E4D0-F4C8-B9DF2A27216B.webp",
                        },
                        text_vertical_bias: 0.5,
                        text_horizontal_bias: 0,
                        title1_text_size: "12",
                        title1_text_color: "#031269",
                        title1_is_bold: true,
                        title2_text_size: "18",
                        title2_text_color: "#1a29a9",
                        title2_is_bold: false,
                        title3: `By ${_.startCase(data[k].expert_name.toLowerCase())}`,
                        title3_text_size: "12",
                        title3_text_color: "#031269",
                        title3_is_bold: false,
                    },
                };
                finalData.push(obj);
                qidKeys[data[k].resource_reference] = true;
            }
        }
        redisClient.set(`LIVE_CLASS_FREE_RECOMMENDED_VIDEO_${sClass}_${language}`, JSON.stringify(finalData), "Ex", 24 * 60 * 60);
    }
}

async function createPopularTopicPlaylist(data, sClass, language) {
    if (data && data.length) {
        console.log("chapter data length", data.length);
        const finalData = [];
        const subjData = data.reduce((prev, curr) => {
            if (curr.subject.toLowerCase()) {
                prev[curr.subject.toLowerCase()] = [...prev[curr.subject.toLowerCase()] || [], curr];
            }
            return prev;
        });
        const subjList = Object.keys(subjData);
        // console.log(subjData);
        for (let k = 0; k < subjList.length; k++) {
            if (subjData[subjList[k]] && subjData[subjList[k]].length && subjData[subjList[k]][0].chapter_assortment_id && subjData[subjList[k]][0].subject !== "announcement") {
                const obj = {
                    deeplink: `doubtnutapp://course_detail_info?assortment_id=${subjData[subjList[k]][0].chapter_assortment_id}&tab=recent`,
                    image_url: `${subjectImage[subjData[subjList[k]][0].subject.toLowerCase()] || subjectImage.default}`,
                    image_bg_color_one: "",
                    image_bg_color_two: "",
                    title: `${subjData[subjList[k]][0].topic}`,
                    title_text_size: "14",
                    title_text_color: "#17181f",
                    title2: `${subjData[subjList[k]][0].count} people are studying this now`,
                    title2_text_size: "12",
                    title2_text_color: "#5b5b5b",
                };
                finalData.push(obj);
            }
        }
        // console.log(finalData);
        redisClient.set(`LIVE_CLASS_FREE_RECOMMENDED_TOPICS_${sClass}_${language}`, JSON.stringify(finalData.slice(0, 3)), "Ex", 24 * 60 * 60);
    }
}

async function setTeachersData(data, sClass, language) {
    console.log("teaches data", data.length);
    if (data && data.length) {
        redisClient.set(`LIVE_CLASS_FREE_TOP_TEACHER_${sClass}_${language}`, JSON.stringify(data), "Ex", 24 * 60 * 60);
    }
}

async function generateFreeLiveclassCache() {
    const sClass = [12, 11, 10, 9, 8, 7, 6, 14];
    const language = ["ENGLISH", "HINDI"];
    for (let i = 0; i < sClass.length; i++) {
        console.log(sClass[i]);
        for (let j = 0; j < language.length; j++) {
            console.log(language[j]);
            const [data, chapterData, teachersData] = await Promise.all([
                getLiveClassVideo(sClass[i], language[j]) || [],
                getLiveClassChapterData(sClass[i], language[j]) || [],
                getTeachersData(sClass[i], language[j]) || [],
            ]);

            await Promise.all([
                createRecommendedPlaylist(data, sClass[i], language[j]),
                createPopularTopicPlaylist(chapterData, sClass[i], language[j]),
                setTeachersData(teachersData, sClass[i], language[j]),
            ]);
        }
    }
}

async function start(job) {
    await generateFreeLiveclassCache();
    await job.progress(100);
    console.log(`the script successfully ran at ${new Date()}`);
    return { data: "success" };
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 1 * * *",
};
