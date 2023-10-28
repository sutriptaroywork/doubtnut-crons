const redisClient = require("../../modules/redis");
const {
    mysql, notification,
} = require("../../modules");

async function getRefreeStudentId() {
    return redisClient.hgetallAsync("REFREE_CHECKOUT_PAGE_VISITED");
}

async function notifSentCountSet(studentID) {
    const todayEnd = new Date().setHours(23, 59, 59, 999);
    return redisClient.multi()
        .hset(`COURSE_NOTIFICATION:${studentID}`, "checkout_notif_count", 1)
        .expireat(`COURSE_NOTIFICATION:${studentID}`, +(todayEnd / 1000))
        .execAsync();
}

async function deleteRefreeStudentId(studentId) {
    return redisClient.hdelAsync("REFREE_CHECKOUT_PAGE_VISITED", `${studentId}`);
}

async function getStudentInfo(sidList) {
    const sql = "SELECT student_id,gcm_reg_id, student_fname, student_lname FROM students where student_id in (?)";
    return mysql.pool.query(sql, [sidList]).then(([res]) => res);
}

async function getStudentPriceEXP(sidList) {
    const sql = "select * from flagr_student_info_with_flag_key where student_id in (?) and flag_key ='referral_package_pricing_experiment_all'";
    return mysql.pool.query(sql, [sidList]).then(([res]) => res);
}

function getParsedData(data) {
    try {
        return JSON.parse(data);
    } catch (e) {
        console.log(e);
        return {};
    }
}

async function sendCourseRefreeClpCheckoutNotifications() {
    const campaignSidData = await getRefreeStudentId();
    const refreeSid = Object.keys(campaignSidData);
    console.log("sid list length", refreeSid.length);
    if (refreeSid.length) {
        const [refreeSidInfo, priceExp] = await Promise.all([
            getStudentInfo(refreeSid),
            getStudentPriceEXP(refreeSid),
        ]);
        const promise = [];
        const studentPriceMap = {};
        for (let i = 0; i < priceExp.length; i++) {
            if (priceExp[i].data) {
                const priceval = getParsedData(priceExp[i].data);
                studentPriceMap[priceExp[i].student_id] = priceval.payload && priceval.payload.key && priceval.payload.key === 2 ? 1999 : 2499;
            }
        }

        for (let i = 0; i < refreeSidInfo.length; i++) {
            if (refreeSidInfo[i].gcm_reg_id) {
                let message = "Aapke liye humare paas hai Rs.2499 ke courses 30% OFF pr!🤞😎 \nTo aaj hi khareedein!";
                if (studentPriceMap[refreeSidInfo[i].student_id] && studentPriceMap[refreeSidInfo[i].student_id] === 1999) {
                    message = "Aapke liye humare paas hai Rs.1999 ke courses 40% OFF pr!🤞😎 \nTo aaj hi khareedein!";
                }
                const payloadObj = {
                    title: "Mauka abhi haath se gaya nahin hai!😨 ",
                    message,
                    data: {
                        deeplink: "doubtnutapp://course_category?category_id=xxxx",
                    },
                    event: "course_refree_referrer_notification",
                    s_n_id: "course_refree_referrer_notification_4",
                };
                promise.push(notification.sendNotification([{ id: refreeSidInfo[i].student_id, gcmId: refreeSidInfo[i].gcm_reg_id }], payloadObj));
                promise.push(notifSentCountSet(refreeSidInfo[i].student_id));
                promise.push(deleteRefreeStudentId(refreeSidInfo[i].student_id));
            }
        }
        await Promise.all(promise);
    }
}

async function start(job) {
    await sendCourseRefreeClpCheckoutNotifications();
    await job.progress(100);
    console.log(`the script successfully ran at ${new Date()}`);
    return { data: "success" };
}

module.exports.start = start;
module.exports.opts = {
    cron: "0/10 * * * *",
};
