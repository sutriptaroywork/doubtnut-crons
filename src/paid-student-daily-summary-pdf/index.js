const moment = require("moment");
const { mysql } = require("../../modules");
const { notification } = require("../../modules");

async function getDetails() {
    const sql = "SELECT a.student_id,b.assortment_id, d.gcm_reg_id, c.locale, e.pdf_id, e.pdf_date, e.pdf_link from (SELECT * FROM `student_package_subscription` where is_active = 1) as a left join package as b on a.new_package_id = b.id left join course_details_liveclass_course_mapping as c on b.assortment_id=c.assortment_id left join students as d on a.student_id=d.student_id left join (select id as pdf_id, date as pdf_date, pdf_link, pdf_created, assortment_id as pdf_assortment_id from last_day_liveclass_pdfs where date(date) = date(subdate(NOW(),interval 1 day)) and pdf_created=1) as e on b.assortment_id=e.pdf_assortment_id where c.vendor_id=1  and e.pdf_id is not null group by a.student_id,b.assortment_id order by b.assortment_id,a.student_id";
    // sql = 'select student_id,17928 as assortment_id,gcm_reg_id, locale from students where student_id in (724515, 4414510, 7232)';
    console.log(sql);

    const users = await mysql.pool.query(sql).then((res) => res[0]);
    return users;
}

async function getLastDayPdf(assortment_id) {
    const sql = `select * from last_day_liveclass_pdfs where assortment_id=${assortment_id} and date(date) = date(subdate(NOW(),interval 1 day)) and pdf_created=1`;
    const users = await mysql.pool.query(sql).then((res) => res[0]);
    return users;
}

async function updateNotificationCount(id, notification_count) {
    const sql = `update last_day_liveclass_pdfs set notif_sent=${notification_count} where id=${id}`;
    const res = await mysql.writePool.query(sql);
    return res;
}

async function start(job) {
    try {
        const assortmentCounts = {};
        const data = await getDetails();
        for (const item of data) {
            console.log(item);
            const student = [];
            const stu = {};
            stu.id = item.student_id;
            stu.gcmId = item.gcm_reg_id;
            student.push(stu);
            const notificationPayload = {
                event: "pdf_viewer",
                title: item.locale.includes("HINDI") ? `क्या आपने अपनी कल की सारी क्लासेस देख ली? | ${moment(item.pdf_date).format("LL")}` : "Kya aapne kal ki saari classes dekh li? ",
                message: item.locale.includes("HINDI") ? "क्लिक करें और पायें कल की लाइव क्लासेस के \n📹 लेक्चर्स \n📃 नोट्स \n📖 होमवर्क\n" : `Dekhen Kal Ki Classes Ki Summary | ${moment(item.pdf_date).format("LL")}\nClick to find\n📹 Lectures\n📃 Notes\n📖 Homework`,
                firebase_eventtag: item.locale.includes("HINDI") ? "LIVECLASSHISUMMARY" : "LIVECLASSENSUMMARY",
                s_n_id: item.locale.includes("HINDI") ? "LIVECLASSHISUMMARY" : "LIVECLASSENSUMMARY",
                data: {
                    pdf_url: `https://d10lpgp6xz60nq.cloudfront.net/${item.pdf_link}`,

                },
            };
            const response = await notification.sendNotification(student, notificationPayload);
            if (assortmentCounts[item.pdf_id]) {
                assortmentCounts[item.pdf_id] = assortmentCounts[item.pdf_id] + 1;
            } else {
                assortmentCounts[item.pdf_id] = 1;
            }
        }
        for (const p in assortmentCounts) {
            if (assortmentCounts.hasOwnProperty(p)) {
                // update the notif count
                await updateNotificationCount(p, assortmentCounts[p]);
            }
        }
        console.log(assortmentCounts);
    } catch (e) {
        return 0;
        console.log(e);
    } finally {
        console.log(`the script successfully ran at ${new Date()}`);
    }
    return 1;
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 7 * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
    skipDelayed: true,
};
