const _ = require("lodash");
const {
    mysql, notification,
} = require("../../modules");

async function getReferrerStudentId() {
    const sql = "SELECT campaign, student_id FROM `campaign_sid_mapping` WHERE created_at >= now() - INTERVAL 1 hour and campaign like 'CEO_REFERRAL;;;%' and is_active=1 ORDER by id DESC";
    return mysql.pool.query(sql).then(([res]) => res);
}

async function getStudentInfo(sidList) {
    const sql = "SELECT student_id,gcm_reg_id, student_fname, student_lname, student_class FROM students where student_id in (?)";
    return mysql.pool.query(sql, [sidList]).then(([res]) => res);
}

async function sendCourseReferrerNotifications() {
    const campaignSidData = await getReferrerStudentId();
    console.log("sid list length", campaignSidData.length);
    const referrerSidList = []; const refreeSid = [];
    for (let i = 0; i < campaignSidData.length; i++) {
        if (campaignSidData[i].campaign && campaignSidData[i].student_id) {
            let sid = campaignSidData[i].campaign.split(";;;");
            sid = sid[1].split("::");
            if (sid.length) {
                referrerSidList.push(+sid[0]);
            }
            refreeSid.push(+campaignSidData[i].student_id);
        }
    }
    if (referrerSidList.length) {
        const [referrerSidInfo, refreeSidInfo] = await Promise.all([
            getStudentInfo(referrerSidList),
            getStudentInfo(refreeSid),
        ]);

        const promise = [];
        for (let i = 0; i < referrerSidInfo.length && refreeSidInfo.length; i++) {
            if (referrerSidInfo[i].gcm_reg_id && _.includes([11, 12, 13], +referrerSidInfo[i].student_class)) {
                const refreeName = refreeSidInfo[i].student_lname && refreeSidInfo[i].student_lname.length ? `${refreeSidInfo[i].student_fname} ${refreeSidInfo[i].student_lname}` : refreeSidInfo[i].student_fname;
                const payloadObj = {
                    title: `Sharing is Caring!💚🧑‍🤝‍🧑 Aapke dost ${refreeName} ne Doubtnut app kiya hai install 🤩`,
                    message: "Toh jaldi se unka admission karwayein unke manpasand course mein 😇",
                    data: {
                        url: "https://forms.gle/AepjXrQHQsUnGxpJ9",
                    },
                    event: "external_url",
                    s_n_id: "course_refree_referrer_notification_2",
                };
                promise.push(notification.sendNotification([{ id: referrerSidInfo[i].student_id, gcmId: referrerSidInfo[i].gcm_reg_id }], payloadObj));
            }
        }
        await Promise.all(promise);
    }
}

async function start(job) {
    await sendCourseReferrerNotifications();
    await job.progress(100);
    console.log(`the script successfully ran at ${new Date()}`);
    return { data: "success" };
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 0/1 * * *",
};
