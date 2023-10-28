/* eslint-disable prefer-const */
/* eslint-disable no-await-in-loop */
const axios = require("axios");
const { mysql } = require("../../modules");
const { redshift, config } = require("../../modules");

async function getStudents() {
    const sql = `Select student_id from (
        SELECT c.install_dt, c.aaid as gaid,a.student_id as student_id,student_class,locale,
        COUNT(DISTINCT case when b.date>=c.install_dt then b.student_id end) as BNB_User
        FROM
        (SELECT to_date(timestamp+ interval '330 minutes','YYYY|MM|DD') as install_dt,latd_campaign,referred_udid,aaid from analytics.branch_events_analysis
        where name IN ('INSTALL') and timestamp + interval '330 minutes'>=CURRENT_DATE-INTERVAL '5 DAYS' and timestamp+interval '330 minutes'<(CURRENT_DATE)
        union 
        SELECT to_date(timestamp_iso+ interval '330 minutes','YYYY|MM|DD') as install_dt,latd_campaign,user_data_android_id ,aaid from analytics.branch_events_hourly 
        where name IN ('INSTALL') and timestamp_iso + interval '330 minutes'>=CURRENT_DATE-INTERVAL '3 DAYS' ) as c
        LEFT JOIN
        (SELECT to_date(curtimestamp+ interval '330 minutes','YYYY|MM|DD') as join_dt,student_class,case when locale like 'en' then 'en'
        when locale like 'hi' then 'hi' else 'others' end as locale,student_id,gaid,udid FROM classzoo1.students where (mobile is NULL or mobile ~ '^[0-9]{10}$') and
        (udid not like '' or udid is not NULL)
        and is_web<>'1' 
        ) as a ON c.aaid=a.gaid
        LEFT JOIN
        (select a.date,a.student_id
        from
        (
        (
        SELECT a.student_id,date(event_time) as date
        FROM analytics.autosales a
        where
        (event_action in ('lc_buy_now_click','lc_course_click_buy_now','lc_bundle_click_buy_now','LC_sheet_click_buy_now')
        or (event_action in ('common_widget_click') and widget_type in ('widget_course_recommendation','widget_course_wallet','widget_recommended_test','widget_course_info_v2','widget_trial_button','widget_buy_complete_course','package_details_v4','widget_course_plan'))
        or (event_action in ('LC_bottom_sheet_view') and ("source" in ('PRE_PURCHASE_BUY_BUTTON','KOTA_EXPLORE')))
        or (event_action in ('LC_bottom_sheet_view') and ("source" in ('HOMEPAGE')) and assortment_id = 138829)
        and (a.assortment_id not like '%scholarship%'  OR a.assortment_id is null or a.assortment_id = ''))
        group by 1,2
        )
        union all
        (
        select student_id,date(event_time) as date
        from analytics.promo_banner
        where id in ('358','359','360','361','362','363','364','365','366','367','368','369',
        '370','371','372','373','374','375','376','377','378','379','380','381','385','386','395')
        group by 1,2
        )
        union all
        (SELECT a.student_id,date(event_time) as date
        FROM analytics.pc_click a
        join classzoo1.course_details  cd on a.assortment_id = cd.assortment_id
        where ((event_action in ('nudge_course_widget_item_click') and widget in ('SrpNudgeCourseWidget'))
        or (event_action in ('package_detail_v4_item_clicked') and source in ('PRE_PURCHASE_BUY_BUTTON'))
        or (event_action in ('subject_course_widget_item_click') and widget_type in ('widget_subject_course_card')))
        and (lower(cd.assortment_type) like '%course%' or lower(cd.assortment_type) like '%subject%')
        group by 1,2
        )
        union all
        (SELECT a.student_id,date(event_time) as date
        FROM analytics.pc_click a
        where ((event_action in ('pc_cta_click') and (clicked_button_name in ('Get Admission') or clicked_button_name in ('एडमिशन लें') or clicked_button_name in ('Buy Now') or clicked_button_name in ('अभी खरीदें')))
        or (event_action in ('pc_cta_click') and widget in  ('PopularCourseWidget')))
        group by 1,2)
        union all
        (select a.studentid,date(eventtime) as date
        from analytics.ncpevents a
        where eventaction in ('ncp_choose_plan_item_clicked','ncp_view_plan_tapped', 'mpvp_course_bottomsheet_trial_clicked',
        'mpvp_course_bottomsheet_buy_now_clicked')
        group by 1,2
        )
        union all
        (select a.student_id,date(eventtime) as date
        from analytics.lc_sticky a
        where event_action in ('lc_sticky_buy_now_button')
        group by 1,2
        )
        union all
        (select a.student_id,date(eventtime) as date
        from analytics.crm_leads a
        where ((eventaction in ('popular_course_cta_clicked'))
        or (eventaction in ('course_recommendation_cta_clicked') and widget_type in ('widget_course_recommendation'))
        or (eventaction in ('continue_buying_cta_clicked') and widget_type in ('live_class_carousel_card_2'))
        or (eventaction in ('wallet_widget_cta_clicked') and widget_type in ('widget_course_wallet'))
        or (eventaction in ('explore_page_strip_preview_buy_now_clicked','top_selling_subject_widget_cta_clicked'))
        or (eventaction in ('course_v4_cta_clicked') and cta_title in ('Get Admission','एडमिशन लें') and widget_title in ('Courses','कोर्स'))
        or (eventaction in ('course_v4_cta_clicked') and cta_title in ('Get Admission','एडमिशन लें') and screen_name in ('ExploreFragment'))
        or (eventaction in ('widget_latest_launches_cta_clicked')))
        group by 1,2
        )
        union all
        (select student_id,date(event_time) as date from analytics.inappsearch_clicksuggestion_date
        where clicked_item like '%Get Admission%'
        group by 1,2
        )
        union all
        (select a.student_id,date(event_time) as date
        from analytics.button_border a
        where eventaction in ('border_button_clicked')
        and cta_text in ('Buy Now','अभी खरीदें')
        group by 1,2)
        )
        a
        where a.date >=CURRENT_DATE-INTERVAL '30 DAYS' and a.date<(CURRENT_DATE)
        group by 1,2
        order by 1) as b ON a.student_id=b.student_id 
        group by 1,2,3,4,5) Where BNB_User='1' and student_class in (9,10,11,12,13) and locale='hi' group by 1`;
    return redshift.query(sql);
}

async function sendEvent(gaid, udid, studentClass, mainKey) {
    let name = "BNB_9-10_HI";
    if (+studentClass >= 11) {
        name = "BNB_11-13_HI";
    }
    const body = {
        name,
        customer_event_alias: "my custom alias",
        user_data: {
            os: "Android",
            os_version: 25,
            environment: "FULL_APP",
            aaid: gaid,
            android_id: "a12300000000",
            limit_ad_tracking: false,
            developer_identity: udid,
            country: "US",
            language: "en",
            ip: "192.168.1.1",
            local_ip: "192.168.1.2",
            brand: "LGE",
            app_version: "1.0.0",
            model: "Nexus 5X",
            screen_dpi: 420,
            screen_height: 1794,
            screen_width: 1080,
        },
        metadata: {},
        branch_key: mainKey,
    };
    const options = {
        url: "https://api2.branch.io/v2/event/custom",
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

async function getIDs(students) {
    const sql = "select student_class, gaid, udid from students where student_id in (?)";
    return mysql.pool.query(sql, [students]).then((res) => res[0]);
}

async function start(job) {
    const mainKey = config.branch_key;
    let students = await getStudents();
    students = students.map((x) => x.student_id);
    const ids = await getIDs(students);
    const promises = [];
    for (let i = 0; i < ids.length; i++) {
        promises.push(sendEvent(ids[i].gaid, ids[i].udid, ids[i].student_class, mainKey));
    }
    await Promise.all(promises);
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 */3 * * *", // Every 3 hours
};
