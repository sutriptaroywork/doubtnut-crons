const { mysql} = require("../../modules/index");
const { redshift } = require("../../modules");

async function getSalesAttributionData(payment_info_id_list) {
    const sql = `SELECT pi2.pi2_id, pi2.student_id, pi2.created_at , pi2.mobile, pi2.amount, pi2.package_id, pi2.total_amount, pi2.wallet_amount, case when tt2.auto_tele is NULL then 'AUTO' else tt2.auto_tele end as auto_tele, case when tt2.bda_name is NULL then 'AUTO' else tt2.bda_name end as bda_name, case when tt2.tl_name is NULL then 'AUTO' else tt2.tl_name end as tl_name, case when tt2.sm_name is NULL then 'AUTO' else tt2.sm_name end as sm_name, case when (lower(tt2.bda_name) like ('agent%') or lower(tt2.bda_name) like ('intern%')) then 'Enser agents' when tt2.bda_name is NULL then 'AUTO' when tt2.auto_tele = 'AUTO' then 'AUTO' else 'DN IH Agents' end as agent_type, case when tt2.osp_status in ('OSP_Sale') then 'OSP_Sale' else 'No OSP Sale' end as OSP_Status, case when tt2.auto_tele in ('TELE') and tt2.osp_status in ('OSP_Sale') then 'OSP_Clash' when (tt2.auto_tele is NULL OR tt2.auto_tele in ('AUTO')) and tt2.osp_status in ('OSP_Sale') then 'OSP_Sale' when tt2.auto_tele is null and tt2.osp_status <> ('OSP_Sale') then 'AUTO' when tt2.auto_tele is null and tt2.osp_status is null then 'AUTO' else tt2.auto_tele end as final_status from (SELECT pi.id as pi2_id, pi.student_id, pi.created_at, pi.variant_id, v.package_id, pi.amount, pi.total_amount, pi.wallet_amount, pi.discount, st1.mobile from classzoo1.payment_info pi left join classzoo1.students st1 on pi.student_id = st1.student_id left join classzoo1.variants v on v.id = pi.variant_id WHERE pi.status = 'SUCCESS' and pi.variant_id is not null and pi.id in (${payment_info_id_list.join()}) ) as pi2 LEFT JOIN (SELECT DISTINCT tt1.* from (SELECT pi1.*, cd.display_name as course_name, cd.assortment_type, calls.call_pickup_time_c as call_time, st1.mobile, case when calls.id is null then 'AUTO' when calls.call_pickup_time_c is NULL then 'AUTO' when cd.assortment_type not in ('course','subject') then 'AUTO' else calls.id end as call_id, calls.disposition_status_c, calls.onboarding_status_c, calls.renewal_status_c, calls.call_recording_url_c, calls.assigned_user_id, calls.call_pickup_time_c, calls.call_hangup_time_c, calls.talk_time_c, case when ddn.mobile_number is null then 0 else rank() over (partition by ddn.mobile_number order by ddn.call_time desc) end as osp_rank, case when ddn.osp_status is not null and cd.assortment_type in ('course','subject') then ddn.osp_status else 'AUTO' end as osp_status, case when calls.id is null then 0 else datediff(days,calls.call_pickup_time_c::timestamp,pi1.created_at::timestamp) end as call_diff, case when calls.id is null then 1 else rank () over(partition by pi1.pi_id order by (case when calls.call_pickup_time_c is null then calls.date_entered else calls.call_pickup_time_c end) desc) end as call_rank, case when calls.id is null then 'AUTO' when cd.assortment_type not in ('course','subject') then 'AUTO' else calls.assigned_user_id end as assigned_user_id, case when u1.user_name is null then 'NULL' when cd.assortment_type not in ('course','subject') then 'AUTO' else u1.user_name end as bda_name, case when u2.user_name is null then 'NULL' when cd.assortment_type not in ('course','subject') then 'AUTO' else u2.user_name end as tl_name, case when u3.user_name is null then 'NULL' when cd.assortment_type not in ('course','subject') then 'AUTO' else u3.user_name end as sm_name, case when calls.id is null then 'AUTO' when cd.assortment_type not in ('course','subject') then 'AUTO' when datediff(days,calls.call_pickup_time_c::timestamp,pi1.created_at::timestamp) >=0 and datediff(days,calls.call_pickup_time_c::timestamp,pi1.created_at::timestamp) <=7 and calls.call_pickup_time_c<=pi1.created_at then 'TELE' else 'AUTO' end as auto_tele from (SELECT id as pi_id, student_id, created_at, variant_id, amount, total_amount, wallet_amount, discount, coupon_code from classzoo1.payment_info pi WHERE status = 'SUCCESS' and variant_id is not null and id in (${payment_info_id_list.join()}) ) as pi1 left join classzoo1.students st1 on pi1.student_id = st1.student_id left join suitecrm.leads ld on st1.mobile = ld.phone_mobile left join (SELECT a.*, b.* from suitecrm.calls as a left join suitecrm.calls_cstm as b on a.id=b.id_c where b.disposition_status_c in ('sale','wfc','p2p','interested') or b.onboarding_status_c in ('sale','wfc','p2p','interested') or b.renewal_status_c in ('sale','wfc','p2p','interested') ) calls on ld.id=calls.parent_id left join (select mobile_number, date, call_start_time as call_time, sub_status, sub_sub_status, status, case when lower(sub_status) like ('%payment done%') or lower(sub_status) like ('%yearly%') or lower(sub_status) like ('%6 months%') or lower(sub_status) like ('%annual%') or lower(sub_status) like ('%2 yearly%') or lower(sub_status) like ('%monthly%') or lower(sub_status) like ('%parents confirmation%') or lower(sub_status) like ('%promise to pay%') or (lower(sub_status) like ('%interested%') and lower(sub_status) not like ('%not interested%')) or lower(sub_status) like ('%p2p%') or lower(sub_status) like ('%quarterly%') or lower(sub_sub_status) like ('%parents confirmation%') or lower(sub_sub_status) like ('%promise to pay%') or lower(status) like ('%payment done%') or lower(status) like ('%6 months%') then 'OSP_Sale' else sub_status end as osp_status from classzoo1.dialer_data_new where date(call_date) >='2020-09-01' and mobile_number ~ '^[0-9]{10}$' and talk_time >0 ) ddn on ld.phone_mobile = ddn.mobile_number and datediff(days, ddn.date::date,date(pi1.created_at)) >= 0 and datediff(days, ddn.date::date,date(pi1.created_at)) <= 7 and ddn.osp_status in ('OSP_Sale') left join classzoo1.variants v on pi1.variant_id=v.id left join classzoo1.package p on v.package_id = p.id left join (SELECT distinct assortment_id, assortment_type, display_name from classzoo1.course_details) cd on p.assortment_id=cd.assortment_id left join suitecrm.users as u1 on calls.assigned_user_id=u1.id left join suitecrm.users as u2 on u1.reports_to_id=u2.id left join suitecrm.users as u3 on u2.reports_to_id=u3.id where calls.call_pickup_time_c<=pi1.created_at order by calls.call_pickup_time_c desc ) tt1 where tt1.call_rank = 1 and tt1.osp_rank <= 1 order by 1 desc ) tt2 on pi2.pi2_id=tt2.pi_id`;
    console.log(sql);
    return await redshift.query(sql).then((res) => res);
}

async function getSuccessfulPaymentInLast2Hr() {
    const sql = `SELECT id FROM payment_info WHERE status= "SUCCESS" and updated_at > now() - interval 2 hour`;
    return mysql.pool.query(sql).then((res) => res[0]);
}
async function insertIntoPaymentSalesAttributionData(obj) {
    const sql = `insert into payment_info_sales_attribution (payment_info_id, package_id, bda_name, tl_name, sm_name, auto_tele, osp_status, final_status)  values('${obj.payment_info_id}', '${obj.package_id}', '${obj.bda_name}', '${obj.tl_name}', '${obj.sm_name}', '${obj.auto_tele}', '${obj.osp_status}', '${obj.final_status}') on duplicate key update package_id = ${obj.package_id}, bda_name = '${obj.bda_name}', sm_name = '${obj.sm_name}', tl_name = '${obj.tl_name}', auto_tele = '${obj.auto_tele}', osp_status = '${obj.osp_status}', final_status= '${obj.final_status}' `;
    console.log(sql);
    return mysql.writePool.query(sql).then((res) => res[0]);
}

async function start(job) {
    const payment_info_id = await getSuccessfulPaymentInLast2Hr();
    if(payment_info_id.length ===0)
        return;

    const payment_info_id_list = payment_info_id.map((pid)=>{ return pid.id})

    console.log(payment_info_id_list);

    const redshiftData = await getSalesAttributionData(payment_info_id_list);

    for(let i=0; i<redshiftData.length;i++)
    {
        await insertIntoPaymentSalesAttributionData({package_id : redshiftData[i].package_id, payment_info_id : redshiftData[i].pi2_id, bda_name : redshiftData[i].bda_name, tl_name: redshiftData[i].tl_name, sm_name: redshiftData[i].sm_name, auto_tele : redshiftData[i].auto_tele, osp_status : redshiftData[i].osp_status, final_status : redshiftData[i].final_status});
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "*/30 * * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
