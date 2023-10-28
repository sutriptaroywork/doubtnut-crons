/**
 * This Cron is used to update the coupon indexes on ES of dynamic coupons every minute
 */
const axios = require("axios");
const { mysql, slack, config } = require("../../modules");

async function getDynamicCouponstoIndex() {
    const sql = "SELECT coupon_code from coupons_new where is_active = 1 and is_dynamic_coupon = 1";
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function updateElasticIndexing(couponList) {
    const options = {
        method: "POST",
        url: "https://panel-api.doubtnut.com/v1/coupon/update-coupon-index",
        data: {
            coupon_codes: couponList.join(","),
        },
    };
    const { data } = await axios(options);
    return data;
}

async function start(job) {
    const blockNew = [];
    try {
        const couponCodesToIndex = await getDynamicCouponstoIndex();
        if (couponCodesToIndex.length) {
            await updateElasticIndexing(couponCodesToIndex);
        }
        return { err: null, data: { done: true } };
    } catch (e) {
        console.error("Error in indexing dynamic coupon", e);
        blockNew.push({
            type: "section",
            text: { type: "mrkdwn", text: `CRON | ALERT!!! Error in indexing dynamic coupons <@U01MJU54A21> <@U0273ABLEPL> <@ULGN432HL>:\n\`\`\`${e.stack}\`\`\`` },
        });
        await slack.sendMessage("#payments-team", blockNew, config.paymentsAutobotSlackAuth);
        return { err: e, data: { done: false } };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "*/1 * * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
