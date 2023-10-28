/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const axios = require("axios");
const { mysql } = require("../../modules");


function getCouponsData() {
    const sql = "select * from coupon_packages_intermediate where DATE(start_date) <= CURRENT_DATE and DATE(end_date) >= CURRENT_DATE and is_active=1";
    return mysql.pool.query(sql).then(([res]) => res);
}

async function updatePercolateIndex(couponList) {
    couponList = couponList.join(',');
    const params = JSON.stringify({"coupon_codes":couponList});
    const { data } = await axios({
        method: 'post',
        url: 'https://panel-api.doubtnut.com/v1/coupon/update-coupon-index',
        headers: {
            'Content-Type': 'application/json',
        },
        data: params,
    });
    console.log(data);
    return data;
}

async function start(job) {
    job.progress(0);
    const con = await mysql.writePool.getConnection();
    const couponsData = await getCouponsData();
    if (couponsData.length) {
        const groupedCouponData = _.groupBy(couponsData, "coupon_code");
        job.progress(50);
        const couponList = [];
        for (const key in groupedCouponData) {
            if (Object.prototype.hasOwnProperty.call(groupedCouponData, key)) {
                couponList.push(key);
                // create bulk insert query
                const bulkArray = [];
                for (let i = 0; i < groupedCouponData[key].length; i++) {
                    bulkArray.push([key, groupedCouponData[key][i].package_id]);
                }
                try {
                    await con.query("START TRANSACTION");
                    await con.query("DELETE FROM coupon_package_id_mapping WHERE coupon_code=? ", [key]);
                    await con.query("INSERT INTO coupon_package_id_mapping (coupon_code, package_id) VALUES ?", [bulkArray]);
                    await con.query("COMMIT");
                    con.release();
                } catch (e) {
                    console.error(e);
                    con.release();
                    return { e };
                }
            }
        }
        if (couponList.length) {
            await updatePercolateIndex(couponList);
        }
    }
    job.progress(100);
    return {
        data: {
            done: true,
        },
    };
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 0 * * *",
};
