/* eslint-disable no-await-in-loop */
const {
    mysql, gupshup, whatsapp, deeplink, notification,
} = require("../../modules");

const event = "TRIAL-END";
const sourceMapping = {
    10: 8400400400,
    11: 6003008001,
};

const templates = {
    8400400400: {},
    6003008001: {},
    notification: {
        en: {
            title: "Aapke course ka free demo khatam hogaya hai! 😎",
            message: "Apne course ki padhai continue karne ke liye aaj hi khareedein.",
            firebaseTag: "TRIAL_END_EN",
        },
        hi: {
            title: "आपके कोर्स का फ़्री डेमो समाप्त हो गया है! 😎",
            message: "अपने कोर्स की पढ़ाई जारी रखने के लिए आज ही ख़रीदें.",
            firebaseTag: "TRIAL_END_HI",
        },
    },
    SMS: {
        en: {
            message: `Doubtnut par apke "{{1}}" course ka free demo khatam ho gaya hai!

Apne course ki padhai continue karne ke liye aaj hi khareedein.
{{2}}`,
        },
        hi: {
            message: `Doubtnut पर आपके "{{1}}" कोर्स का फ़्री डेमो समाप्त हो गया है!

अपने कोर्स की पढ़ाई जारी रखने के लिए आज ही ख़रीदें.
{{2}}`,
        },
    },
};

async function fetchStudents() {
    const sql = `select p.assortment_id, cd.display_name, cd.demo_video_thumbnail, cd.parent, s.student_id, s.gcm_reg_id, s.mobile, cd.meta_info as locale, wo.source, sps.end_date from student_package_subscription sps
    left join whatsapp_optins wo on sps.student_id=wo.id
    left join students s on s.student_id=sps.student_id
    left join package p on sps.new_package_id=p.id
    left join course_details cd on p.assortment_id=cd.assortment_id
    where sps.amount = -1 and sps.end_date between NOW() - INTERVAL 24 HOUR and NOW() and p.assortment_id is not NULL
    group by 1,2,3,4,5,6,7,8`;
    return mysql.pool.query(sql).then(([res]) => res);
}

async function start(job) {
    const sentUsers = [];
    const students = await fetchStudents();

    for (let i = 0; i < students.length; i++) {
        try {
            const row = students[i];
            const phone = `91${row.mobile}`;
            if (sentUsers.includes(row.student_id)) {
                console.log("skip", row.student_id);
                continue;
            }
            const locale = row.locale === "HINDI" ? "hi" : "en";
            const dl = await deeplink.generateDeeplinkFromAppDeeplink("SMS", event, row.parent == 4 ? "doubtnutapp://course_category?category_id=Kota Classes" : `doubtnutapp://course_details?id=${row.assortment_id}&referrer_student_id=`);
            if (row.mobile) {
                const msgTemplate = templates.SMS[locale].message;
                const msg = msgTemplate.replace("{{1}}", row.display_name).replace("{{2}}", dl.url);
                await gupshup.sendSms({ phone, msg, locale });
            }

            if (row.gcm_reg_id) {
                const notificationPayload = {
                    event: "course_details",
                    image: row.demo_video_thumbnail || "",
                    title: templates.notification[locale].title.replace("{{1}}", locale == "hi" ? `कक्षा ${row.class}` : `Class ${row.class}`),
                    message: templates.notification[locale].message,
                    firebase_eventtag: templates.notification[locale].firebaseTag,
                    s_n_id: templates.notification[locale].firebaseTag,
                    // data: JSON.stringify({
                    //     id: row.assortment_id,
                    // }),
                };
                if (row.parent == 4) {
                    notificationPayload.event = "course_category";
                    notificationPayload.data = JSON.stringify({
                        category_id: "Kota Classes",
                    });
                } else {
                    notificationPayload.data = JSON.stringify({
                        id: row.assortment_id,
                    });
                }
                notification.sendNotification([{ id: row.student_id, gcmId: row.gcm_reg_id }], notificationPayload);
            }

            // const whatsappTemplates = templates[sourceMapping[row.source || 10]][locale];
            // const source = sourceMapping[row.source];
            // for (let j = 0; j < whatsappTemplates.length; j++) {
            //     const t = whatsappTemplates[j];
            //     const msg = typeof t.data === "string"
            //         ? t.data.replace("{{1}}", row.name).replace("{{2}}", `doubtnutapp://course_details?id=${row.assortment_id}`)
            //         : t.data.caption.replace("{{1}}", row.name).replace("{{2}}", `doubtnutapp://course_details?id=${row.assortment_id}`);
            //     let hsmData;
            //     if (row.source && t.transactional) {
            //         hsmData = {
            //             sources: {
            //                 [source]: typeof t.data === "string" ? t.data : t.data.caption,
            //             },
            //             attributes: [
            //                 row.name,
            //                 `doubtnutapp://course_details?id=${row.assortment_id}`,
            //             ],
            //         };
            //     }
            //     if (typeof t.data === "string") {
            //         whatsapp.sendTextMsg(event, phone, row.student_id, msg, hsmData);
            //     } else {
            //         whatsapp.sendMediaMsg(event, phone, row.student_id, row.demo_video_thumbnail, t.data.mediaType, msg, hsmData);
            //     }
            // }
            await job.progress(parseInt(((i + 1) * 100) / students.length));
            sentUsers.push(row.student_id);
        } catch (e) {
            console.error(e);
        }
    }
    return { data: null };
}

module.exports.start = start;
module.exports.opts = {
    cron: "15 9 * * *",
    removeOnComplete: 10,
    removeOnFail: 20,
};
