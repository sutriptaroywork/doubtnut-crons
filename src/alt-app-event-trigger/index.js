const axios = require("axios");
const moment = require("moment");
const { redshift, config } = require("../../modules");

const mainKey = config.branch_key;
const iitKey = config.branch_key_iit;
const neetKey = config.branch_key_neet;

const eventList = [
    "backend_open_branch",
    "backend_vv_branch",
    "backend_qa_branch",
    "backend_vv_9to12_branch",
    "backend_qa_9to12_branch",
    "backend_reg_branch",
    "backend_pvb_branch",
    "backend_reg_9to12_branch",
    "backend_open_9to12_branch",
    "backend_pvb_9to12_branch",
];

async function getJeeAppUsers(time) {
    const sql = `SELECT to_date(timestamp_iso+ interval '330 minutes','YYYY|MM|DD') as install_dt,aaid as gaid,user_data_android_id as udid from analytics.branch_events_iit_jee_app where name in ('INSTALL','REINSTALL')  and to_date(timestamp_iso+ interval '330 minutes','YYYY|MM|DD')>= '${time}'`;
    return redshift.query(sql);
}

async function getNeetAppUsers(time) {
    const sql = `SELECT to_date(timestamp_iso+ interval '330 minutes','YYYY|MM|DD') as install_dt,aaid as gaid,user_data_android_id as udid from analytics.branch_events_neet_app where name in ('INSTALL','REINSTALL')  and to_date(timestamp_iso+ interval '330 minutes','YYYY|MM|DD')>= '${time}'`;
    return redshift.query(sql);
}

async function getUAECampaignAppUsers(time) {
    const sql = `select to_date(timestamp+ interval '330 minutes','YYYY|MM|DD') as install_dt, aaid as gaid,referred_udid as udid  from analytics.branch_events_analysis where name in ('INSTALL','REINTALL') and latd_campaign like '%UAE%' and to_date(timestamp+ interval '330 minutes','YYYY|MM|DD')>='${time}'  
	union 
	SELECT to_date(timestamp_iso+ interval '330 minutes','YYYY|MM|DD') as install_dt, aaid  as gaid, user_data_android_id as udid from analytics.branch_events_hourly where name in ('INSTALL','REINSTALL') and latd_campaign like '%UAE%' and timestamp_iso+interval '330 minutes'>='${time}'`;
    return redshift.query(sql);
}

async function sendEvent(eventName, gaid, udid, key) {
    try {
        const payload = {
            name: eventName,
            user_data: {
                os: "Android",
                os_version: 25,
                environment: "FULL_APP",
                aaid: gaid,
                limit_ad_tracking: false,
                developer_identity: udid,
                country: "US",
                language: "en",
                ip: "192.168.1.1",
                local_ip: "192.168.1.2",
                app_version: "1.0.0",
            },
            branch_key: key,
        };

        const options = {
            url: "https://api2.branch.io/v2/event/custom",
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            data: JSON.stringify(payload),
        };

        console.log(options);

        const response = (await axios(options)).data;
        return response;
    } catch (e) {
        console.error(e);
    }
}

async function triggerEvents(users, key) {
    for (let i = 0; i < users.length; i++) {
        try {
            const { gaid } = users[i];
            const { udid } = users[i];
            const promise = [];
            for (let j = 0; j < eventList.length; j++) {
                const event = eventList[j];
                promise.push(sendEvent(event, gaid, udid, key));
            }
            // eslint-disable-next-line no-await-in-loop
            await Promise.all(promise);
        } catch (e) {
            console.log(e);
        }
    }
}

async function start(job) {
    try {
        const time = moment().add(5, "hours").add(30, "minutes").subtract(5, "days")
            .format("YYYY-MM-DD");
        const [jeeAppUsers, neetAppUsers, uaeAppUsers] = await Promise.all([
            getJeeAppUsers(time),
            getNeetAppUsers(time),
            getUAECampaignAppUsers(time),
        ]);
        await Promise.all([
            triggerEvents(jeeAppUsers, iitKey),
            triggerEvents(neetAppUsers, neetKey),
            triggerEvents(uaeAppUsers, mainKey),
        ]);
        return { err: null, data: null };
    } catch (e) {
        console.log(e);
        return { e };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 */3 * * *",
};
