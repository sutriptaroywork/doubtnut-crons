const _ = require('lodash');
const { redis } = require("../../modules/index");
const { mysql } = require("../../modules");

async function getQuizNotificationData() {
    const sql = "select * from dn_adv_vendor_banner_data where is_active = 1 and start_date <= CURRENT_TIMESTAMP and end_date >= CURRENT_TIMESTAMP";
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getQuizNotificationDefaultData() {
    const sql = "select * from quiz_notification_data where is_active = 1";
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getECMData() {
    const sql = "select * from exam_category_mapping where is_active = 1";
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function generateRedisQuizNotificationDefault(data) {
    return redis.setAsync(`qnd:${data.notif_day}`, JSON.stringify(data), "Ex", 60 * 60 * 24);
}

async function generateRedisQuizNotification(data) {
    if (data.feature_id == 11 || data.feature_id == 12) {
        return redis.setAsync(`quiz_notif:${data.ccm_id}_${data.feature_id}`, JSON.stringify(data), "Ex", 60 * 60 * 4);
    }
    return;
}

async function generateECM(key, data) {
    const values = data.map((x) => x.category);
    return redis.setAsync(`ecm:${key}`, JSON.stringify(values), "Ex", 60 * 60 * 24);
}

async function generateRedisQuizNotificationDefaultLast(data) {
    return redis.setAsync(`qnd:last`, JSON.stringify(data), "Ex", 60 * 60 * 24);
}

async function generateQuizNotificationData() {
    const data = await getQuizNotificationData();
    for (let i = 0; i < data.length; i++) {
        await generateRedisQuizNotification(data[i]);
    }
    return;
}

async function generateQuizNotificationDefaultData() {
    const data = await getQuizNotificationDefaultData();
    for (let i = 0; i < data.length; i++) {
        await generateRedisQuizNotificationDefault(data[i]);
    }

    await generateRedisQuizNotificationDefaultLast(data.slice(-2));
    return;
}

async function generateECMData() {
    let data = await getECMData();
    data = _.groupBy(data, 'exam');
    const promises = [];
    Object.keys(data).forEach((key) => promises.push(generateECM(key, data[key])));
    await Promise.all(promises);
    return;
}

module.exports = {
    generateQuizNotificationData,
    generateQuizNotificationDefaultData,
    generateECMData,
};
