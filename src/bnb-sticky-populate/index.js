/* eslint-disable prefer-const */
/* eslint-disable no-await-in-loop */
const axios = require("axios");
const { mysql } = require("../../modules");
const { redshift, config } = require("../../modules");
const { slack } = require("../../modules");

async function truncateTable() {
    const query = 'truncate table dev.campaign_sticky';
    return redshift.query(query);
}

async function insertIntoCampaignSticky() {
    const query = `insert into dev.campaign_sticky
    (select fin.student_id, fin.gcm_reg_id, fin.app_version, fin.is_online, fin.locale, fin.latd_campaign,fin.student_class, inst_date,
    sum(is_iit_user) is_iit_user , sum(is_neet_user) is_neet_user,sum(is_board_user) is_board_user
    from
    (
    select a.student_id, a.gcm_reg_id, a.app_version, a.is_online, a.locale, c.latd_campaign,a.student_class,
    (case when ccm_id in (select id from classzoo1.class_course_mapping ccm where course ='IIT JEE' and ccm.is_active=1) then 1 end) as is_iit_user,
    (case when ccm_id in (select id from classzoo1.class_course_mapping ccm where course ='NEET' and ccm.is_active=1) then 1 end) as is_neet_user,
    (case when ccm_id in (select id from classzoo1.class_course_mapping ccm where category ='board' and ccm.is_active=1) then 1 end) as is_board_user,
    min(c.install_dt) as inst_date
    FROM
    (
    SELECT to_date(timestamp+ interval '330 minutes','YYYY|MM|DD') as install_dt, latd_campaign, referred_udid, aaid
    from analytics.branch_events_analysis
    where name IN ('INSTALL','REINSTALL')and latd_campaign IN (select campaign from classzoo1.campaign_redirection_mapping crm
    where description ='bnb_campaign_vijay')
    and timestamp + interval '330 minutes'>= '2022-04-20' and timestamp+interval '330 minutes'<(CURRENT_DATE)
    union
    SELECT to_date(timestamp_iso+ interval '330 minutes','YYYY|MM|DD') as install_dt,latd_campaign,user_data_android_id, aaid
    from analytics.branch_events_hourly
    where name IN ('INSTALL','REINSTALL') and latd_campaign IN (select campaign from classzoo1.campaign_redirection_mapping crm
    where description ='bnb_campaign_vijay')
    and timestamp_iso + interval '330 minutes'>=CURRENT_DATE-INTERVAL '2 DAYS'
    ) as c
    LEFT JOIN
    (
    SELECT to_date(curtimestamp+ interval '330 minutes','YYYY|MM|DD') as join_dt,student_class,case when locale like 'en' then 'en'
    when locale like 'hi' then 'hi' else 'others' end as locale,student_id,gaid,udid, gcm_reg_id, app_version, is_online
    FROM classzoo1.students
    ) as a
    ON c.aaid=a.gaid
    left join classzoo1.student_course_mapping_2 scm
    on a.student_id=scm.student_id
    where a.student_id is not null and a.gaid <>'00000000-0000-0000-0000-000000000000'
    and c.aaid <>'00000000-0000-0000-0000-000000000000'
    group by 1,2,3,4,5,6,7,8,9,10
    ) as fin
    group by 1,2,3,4,5,6,7,8);`;
    return redshift.query(query);
}

async function getStudents() {
    const query = `select null as id,*,1 as is_active,current_timestamp AT TIME ZONE 'IST' as created_at,current_timestamp AT TIME ZONE 'IST' as updated_at 
    from dev.campaign_sticky 
    where inst_date =current_date -4`;
    return redshift.query(query);
}

async function insertBnb(item) {
    const query = `insert into bnb_campaign_users set ?`;
    return mysql.writePool.query(query, [item]);
}

async function sendEvent(couponCode) {
    const body = {
        coupon_codes: couponCode,
    };
    const options = {
        url: "https://panel-api.doubtnut.com/v1/coupon/update-coupon-index",
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        data: JSON.stringify(body),
    };

    console.log(options);

    const response = (await axios(options)).data;
    return response;
}

async function start(job) {
    await truncateTable();
    await insertIntoCampaignSticky();
    const data = await getStudents();
    for (let i = 0; i < data.length; i += 100) {
        let promises = [];
        for (let j = 0; j < 100; j++) {
            if (i + j < data.length) {
                data[i + j].install_date = data[i + j].inst_date;
                delete data[i + j].inst_date;
                promises.push(insertBnb(data[i + j]));
            }
        }
        await Promise.all(promises);
    }
    const codes = ["DHAMAKA40", "LAST30", "FINAL40"];
    const promises = [];
    for (let i = 0; i < codes.length; i++) {
        promises.push(sendEvent(codes[i]));
    }
    await Promise.all(promises);
    const blockNew = [];
    blockNew.push({
        type: "section",
        text: { type: "mrkdwn", text: `bnb_campaign_users ${data.length}` },
    },
    {
        type: "section",
        text: { type: "mrkdwn", text: `coupon codes ${codes}` },
    });
    await slack.sendMessage("#bnb-cron-update", blockNew, config.SCHOLARSHIP_SLACK_AUTH);
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "30 21 * * *", // Every day at 3 am
};
