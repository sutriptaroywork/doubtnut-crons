const _ = require("lodash");
const redisClient = require("../../modules/redis");
const {
    mysql, notification,
} = require("../../modules");

async function getRefereeStudentId() {
    const sql = "SELECT campaign, student_id FROM `campaign_sid_mapping` WHERE created_at >= now() - INTERVAL 10 minute and campaign like 'ceo_referral_dummy;;;%' and is_active=1 GROUP by student_id";
    return mysql.pool.query(sql).then(([res]) => res);
}

async function getStudentInfo(sidList) {
    const sql = "SELECT student_id,gcm_reg_id, student_fname, student_lname, student_class FROM students where student_id in (?)";
    return mysql.pool.query(sql, [sidList]).then(([res]) => res);
}

async function notifSentCountAdd(studentID) {
    const todayEnd = new Date().setHours(23, 59, 59, 999);
    return redisClient.multi()
        .sadd("ceo_referral_dummy", studentID)
        .expireat("ceo_referral_dummy", parseInt(todayEnd / 1000))
        .execAsync();
}

async function getStudentNotifSentList() {
    return redisClient.smembersAsync("ceo_referral_dummy");
}

async function sendCourseReferrerNotifications() {
    const [campaignSidData, notifSentSidList] = await Promise.all([
        getRefereeStudentId(),
        getStudentNotifSentList(),
    ]);
    console.log("sid list length", campaignSidData.length, notifSentSidList.length);
    const referrerSidList = []; const refreeSid = [];
    for (let i = 0; i < campaignSidData.length; i++) {
        if (campaignSidData[i].campaign && campaignSidData[i].student_id && (!notifSentSidList || !_.includes(notifSentSidList, `${campaignSidData[i].student_id}`))) {
            let sid = campaignSidData[i].campaign.split(";;;");
            sid = sid[1].split("::");
            if (sid.length) {
                referrerSidList.push(+sid[0]);
            }
            refreeSid.push(+campaignSidData[i].student_id);
        }
    }
    console.log(referrerSidList.length, refreeSid.length);
    if (refreeSid.length) {
        const [referrerSidInfo, refreeSidInfo] = await Promise.all([
            getStudentInfo(referrerSidList),
            getStudentInfo(refreeSid),
        ]);
        const referrerSidInfoData = {};
        referrerSidInfo.forEach(x => {
            if(x.student_id){
                referrerSidInfoData[x.student_id] = x;
            }
        });
        // console.log(referrerSidInfoData);

        const promise = [];
        for (let i = 0; i < refreeSidInfo.length && referrerSidInfo.length; i++) {
            if (refreeSidInfo[i].gcm_reg_id && _.includes([11, 12, 13], +refreeSidInfo[i].student_class)) {
                let referrerName = referrerSidInfoData[referrerSidList[i]].student_fname && referrerSidInfoData[referrerSidList[i]].student_fname.length ? referrerSidInfoData[referrerSidList[i]].student_fname : "";
                referrerName = referrerSidInfoData[referrerSidList[i]].student_lname && referrerSidInfoData[referrerSidList[i]].student_lname.length ? `${referrerName} ${referrerSidInfoData[referrerSidList[i]].student_lname}` : referrerName;
                const payloadObj = {
                    title: `Aapke dost ${referrerName} ne aapko Referr kiya hai! 📲`,
                    message: "Paayein 3 mahine ke Courses 30% off par.Wo bhi ₹2500 se bhi kam daam mein hai!😮💵",
                    data: {
                        deeplink: "doubtnutapp://course_category?category_id=xxxx",
                    },
                    event: "course_refree_referrer_notification",
                    s_n_id: "course_refree_referrer_notification_5",
                };
                // console.log(payloadObj);
                promise.push(notification.sendNotification([{ id: refreeSidInfo[i].student_id, gcmId: refreeSidInfo[i].gcm_reg_id }], payloadObj));
                promise.push(notifSentCountAdd(refreeSidInfo[i].student_id));
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
    cron: "0/10 * * * *",
};
