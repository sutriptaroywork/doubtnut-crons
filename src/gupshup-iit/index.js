const { gupshup } = require("../../modules");
const { redshift } = require("../../modules");

async function students2() {
    const sql = `select distinct t2.mobile
    from
    (
    SELECT a.*,b.assortment_id,c.display_name,c.assortment_type,d.liveclass_course_id as old_course_id,
    e.emi_order, e.package_amount,e.amount_paid,e.discount as ps_discount,
    cs.super_cat , c.class , f.total_amount - f.discount as booked_amount,
    CASE WHEN b.duration_in_days <32 THEN '1 month' ELSE 'long duration' END AS type
    from
    (
    SELECT min(id) as subscription_id, student_id,student_package_id, new_package_id,
    variant_id,amount,sps.start_date, sps.end_date,min(date(created_at)) as created_at
    FROM classzoo1.student_package_subscription sps
    where amount >=0
    and student_id not in (SELECT student_id from classzoo1.internal_subscription)
    group by sps.student_id,sps.student_package_id, sps.new_package_id,sps.variant_id,sps.amount, sps.start_date,sps.end_date
    ) as a
    left join classzoo1.package as b on a.new_package_id = b.id
    left join
    (
    SELECT assortment_id,assortment_type,display_name,category, meta_info, min(class)
    WITHIN GROUP (ORDER BY assortment_id) AS class
    from classzoo1.course_details
    group by assortment_id,assortment_type,display_name,category, meta_info
    ) as c on b.assortment_id=c.assortment_id
    left join classzoo1.course_details_liveclass_course_mapping as d on b.assortment_id=d.assortment_id
    left join classzoo1.payment_summary as e on a.subscription_id = e.subscription_id
    left join classzoo1.category_supercategory cs on c.category = cs.category
    left join classzoo1.payment_info f on e.txn_id = f.partner_txn_id
    where
    f.status  = 'SUCCESS' and
    (trim(lower(e.coupon_code)) <> ('internal') or e.coupon_code is null)
    and a.amount + f.wallet_amount >1
    and cs.super_cat in ('IIT JEE')
    and c.meta_info in ('HINDI')
    and a.created_at >= '2020-10-10'
    ) as t1
    left join classzoo1.students as t2 on t1.student_id = t2.student_id
    and t2.mobile is not null`;
    const users = await redshift.query(sql).then((res) => res);
    return users;
}

async function students1() {
    const sql = `select distinct t2.mobile
    from
    (
    SELECT a.*,b.assortment_id,c.display_name,c.assortment_type,d.liveclass_course_id as old_course_id,
    e.emi_order, e.package_amount,e.amount_paid,e.discount as ps_discount,
    cs.super_cat , c.class , f.total_amount - f.discount as booked_amount,
    CASE WHEN b.duration_in_days <32 THEN '1 month' ELSE 'long duration' END AS type
    from
    (
    SELECT min(id) as subscription_id, student_id,student_package_id, new_package_id,
    variant_id,amount,sps.start_date, sps.end_date,min(date(created_at)) as created_at
    FROM classzoo1.student_package_subscription sps
    where amount >=0
    and student_id not in (SELECT student_id from classzoo1.internal_subscription)
    group by sps.student_id,sps.student_package_id, sps.new_package_id,sps.variant_id,sps.amount, sps.start_date,sps.end_date
    ) as a
    left join classzoo1.package as b on a.new_package_id = b.id
    left join
    (
    SELECT assortment_id,assortment_type,display_name,category, meta_info, min(class)
    WITHIN GROUP (ORDER BY assortment_id) AS class
    from classzoo1.course_details
    group by assortment_id,assortment_type,display_name,category, meta_info
    ) as c on b.assortment_id=c.assortment_id
    left join classzoo1.course_details_liveclass_course_mapping as d on b.assortment_id=d.assortment_id
    left join classzoo1.payment_summary as e on a.subscription_id = e.subscription_id
    left join classzoo1.category_supercategory cs on c.category = cs.category
    left join classzoo1.payment_info f on e.txn_id = f.partner_txn_id
    where
    f.status  = 'SUCCESS' and
    (trim(lower(e.coupon_code)) <> ('internal') or e.coupon_code is null)
    and a.amount + f.wallet_amount >1
    and cs.super_cat in ('IIT JEE')
    and c.meta_info in ('ENGLISH')
    and a.created_at >= '2020-10-10'
    ) as t1
    left join classzoo1.students as t2 on t1.student_id = t2.student_id
    and t2.mobile is not null`;
    const users = await redshift.query(sql).then((res) => res);
    return users;
}

async function start(job) {
    try {
        // const studentDetails1 = await students1();
        // const studentDetails2 = await students2();
        const studentDetails1 = [{ mobile: "9182940373" }];
        const studentDetails2 = [{ mobile: "8107395041" }];
        const message1 = "Dear Student,\nCongratulations on your wonderful performance in JEE Exam! Share your details here - https://tiny.doubtnut.com/969x4h6a";
        const message2 = "प्रिय छात्र/ छात्रा,\nJEE परीक्षा में अच्छे प्रदर्शन के लिए बधाई! अपनी अतिरिक्त जानकारी साझा करें और इनाम के रूप में पायें डाउटनट की तराफ से टीशर्ट।\n\nअपनी जानकारी यहां भरें - https://tiny.doubtnut.com/969x4h6a";
        const message3 = "Aapne IIT scholarship test ke liye Doubtnut par register kiya hai.\nAapka test 2021-08-09 ko hai.\nTest link :https://tiny.doubtnut.com/969x4h6a";
        const message4 = "आपने IIT स्कॉलरशिप परीक्षा के लिए Doubtnut पर रजिस्टर किया है|\nआपकी परीक्षा 2021-08-09  को है|\nपरीक्षा का लिंक : https://tiny.doubtnut.com/969x4h6a\n";
        for (let i = 0; i < studentDetails1.length; i++) {
            // eslint-disable-next-line no-await-in-loop
            await gupshup.sendSMSMethodGet({ phone: studentDetails1[i].mobile, msg: message1, locale: "en" });
        }
        for (let i = 0; i < studentDetails2.length; i++) {
            // eslint-disable-next-line no-await-in-loop
            await gupshup.sendSMSMethodGet({ phone: studentDetails2[i].mobile, msg: message2, locale: "hi" });
        }
        for (let i = 0; i < studentDetails2.length; i++) {
            // eslint-disable-next-line no-await-in-loop
            await gupshup.sendSMSMethodGet({ phone: studentDetails2[i].mobile, msg: message3, locale: "en" });
        }
        for (let i = 0; i < studentDetails2.length; i++) {
            // eslint-disable-next-line no-await-in-loop
            await gupshup.sendSms({ phone: studentDetails2[i].mobile, msg: message4, locale: "hi" });
        }
        job.progress(100);
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
    cron: "03 17 9 8 *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
