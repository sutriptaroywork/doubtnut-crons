const { redshift } = require("../../modules");
const messages = require("../../modules/messages");
const flagr = require("../../modules/flagr");

function getDetails() {
    const sql = "SELECT DISTINCT case when x.day_num>=7 then x.student_id end as day_ge_7_user,x.udid,x.gaid,x.mobile,x.locale FROM (SELECT a.dt,a.dt-b.join_dt as day_num,a.student_id,b.gcm_reg_id,b.gaid,b.udid,b.mobile, b.locale from (SELECT to_date(uninstall_timestamp,'YYYY|MM|DD') as dt,student_id from classzoo1.retarget_student_churn WHERE reinstall_timestamp is NULL and uninstall_timestamp >=current_date-interval '1 day' and uninstall_timestamp<current_date) AS a LEFT JOIN (SELECT to_date(curtimestamp + interval '330 minutes','YYYY|MM|DD') as join_dt,student_id,udid,gcm_reg_id,gaid,mobile,locale from classzoo1.students WHERE is_web<>'1') as b ON a.student_id=b.student_id where b.student_id is NOT NULL and b.gaid is NOT NULL ) as x";
    return redshift.query(sql).then((res) => res);
}

async function start(job) {
    try {
        job.progress(10);
        const flgrData = { body: { capabilities: { reinstall_sms: {} }, entityId: 45917205 } };
        const flagrResp = await flagr.getFlagrResp(flgrData, 5000);
        console.log(flagrResp, " flagr response");
        job.progress(20);
        if (flagrResp && flagrResp.reinstall_sms.enabled) {
            console.log("querying from redshift");
            const data = await getDetails();
            job.progress(30);
            let totalSent = 0;
            for (const user of data) {
                // eslint-disable-next-line no-await-in-loop
                await messages.sendSms({
                    phone: user.mobile,
                    msg: (user.locale === "hi" ? "डाउटनट के साथ हो जाओ डाउट फ्री! करो दोबारा से इनस्टॉल, क्योंकि यहाँ है आपकी हर प्रॉब्लम का परफेक्ट सॉल्यूशन! लिंक पर क्लिक करें! https://tinyurl.com/3477sa6k" : "Doubtnut ke saath ho jaao DoubtFree! Karo dobara install Kyunki yahan hai aapki problem ka perfect solution! Turan download karein! https://tinyurl.com/3477sa6k"),
                    locale: user.locale,
                });
                totalSent += 1;
                console.log("SENT on => ", user.mobile, user.locale);
            }
            job.progress(90);
            await messages.sendSms({
                phone: 8588829810,
                msg: `Total ${totalSent} Reinstall Target SMS Sent Today`,
            });
            await messages.sendSms({
                phone: 8699616342,
                msg: `Total ${totalSent} Reinstall Target SMS Sent Today`,
            });
        } else {
            await messages.sendSms({
                phone: 8699616342,
                msg: "Reinstall Target SMS not sent due to flagr fail",
            });
            await messages.sendSms({
                phone: 8588829810,
                msg: "Reinstall Target SMS not sent due to flagr fail",
            });
        }
    } catch (err) {
        console.log(err);
        return { err };
    } finally {
        console.log(`The script successfully ran at ${new Date()}`);
    }
    job.progress(100);
    return 1;
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 7 * * *",
};
