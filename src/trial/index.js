/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const moment = require("moment");
const {
    mysql, kafka,
} = require("../../modules");
const flagr = require("../../modules/flagr");

const templates = {
    notification: {
        en: {
            title: "Payment mein Issue? Padhai rukni nahi chahiye!",
            message: "Doubtnut ne kar diya hai {{1}} ka 3 day trial activate.",
            firebaseTag: "PAYMENT_FAILURE_TRIAL_EN",
        },
        hi: {
            title: "पेमेंट में दिक्कत? पढाई रुकनी नहीं चाहिए!",
            message: "Doubtnut ने कर दिया है {{1}} का तीन दिन का ट्रायल एक्टिवटे!",
            firebaseTag: "PAYMENT_FAILURE_TRIAL_HI",
        },
    },
};

async function getFlagrResponseBool(studentID, expName) {
    try {
        const flgrData = { body: { capabilities: { "3_days_trial_experiment": { entityId: studentID.toString() } }, entityId: studentID.toString() } };
        const flagrResp = await flagr.getFlagrResp(flgrData);
        console.log("flagrResp");
        console.log(flagrResp);
        if (!flagrResp) {
            return false;
        }
        return flagrResp[expName].payload.enabled;
    } catch (e) {
        return true;
    }
}

async function fetchStudents() {
    const sql = `select 
    distinct
    count_table.variant_id,
    package.id,
    cd.display_name,
    cd.parent,
    count_table.student_id,
    s.gcm_reg_id,
    s.mobile,
    cd.meta_info as locale,
    cd.assortment_id,
    s.app_version,
    s.is_online
  from
    (
       select
          id,
          payment_info.student_id,
          count_success,
          payment_for,
          created_at,
          variant_id
       from
          payment_info
          join
             (
                select
                   student_id,
                   sum(if(status = 'SUCCESS', 1, 0)) as count_success
                from
                   payment_info
                   left join
                      variants
                      on variants.id = variant_id
                   left join
                      package
                      on package_id = package.id
                   left join
                      course_details cd
                      on package.assortment_id = cd.assortment_id
                where
                   cd.assortment_type = 'course'
                group by
                   student_id
             )
             as a
             on a.student_id = payment_info.student_id
    )
    as count_table
    left join
       payment_failure
       on count_table.id = payment_failure.payment_info_id
    left join
       variants
       on variants.id = variant_id
    left join
       package
       on package_id = package.id
    left join
       students s
       on s.student_id = count_table.student_id
    left join
       course_details cd
       on package.assortment_id = cd.assortment_id
  where
    count_table.count_success = 0
    and cd.assortment_type = 'course'
    and count_table.created_at < NOW() - INTERVAL 30 minute
    and count_table.created_at > NOW() - INTERVAL 60 minute
    and package.assortment_id is not null
    and s.app_version is not null
    and s.is_online is not null
    and s.student_id not in
    (
       select
          sps.student_id
       from
          student_package_subscription sps
          left join
             package p
             on sps.new_package_id = p.id
       where
          sps.amount = - 1
          and not (sps.variant_id is null
          and sps.new_package_id is null)
    )  `;
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function startSubscriptionForStudentId(obj) {
    const sql = "insert into student_package_subscription SET ?";
    return mysql.pool.query(sql, [obj]).then((res) => res[0]);
}

async function createSubscriptionEntryForTrial(
    studentId,
    variantId,
    packageId,
    amount,
    trialDuration,
) {
    try {
        const now = moment().add(5, "hours").add(30, "minutes");
        const insertSPS = {};
        insertSPS.student_id = studentId;

        insertSPS.variant_id = variantId;
        insertSPS.start_date = moment(now)
            .startOf("day")
            .format("YYYY-MM-DD HH:mm:ss");
        insertSPS.end_date = moment(now)
            .add(trialDuration, "days")
            .endOf("day")
            .format("YYYY-MM-DD HH:mm:ss");
        insertSPS.amount = amount;
        insertSPS.student_package_id = 0;
        insertSPS.new_package_id = packageId;
        insertSPS.doubt_ask = -1;
        insertSPS.is_active = 1;
        return await startSubscriptionForStudentId(insertSPS);
    } catch (e) {
        console.log(e);
        throw e;
    }
}

async function start(job) {
    try {
        job.progress(0);
        const sentUsers = [];
        const students = await fetchStudents();
        console.log(students);
        for (let i = 0; i < students.length; i++) {
            const row = students[i];
            const versionCode = row.is_online;

            if (sentUsers.includes(row.student_id)) {
                console.log("skip", row.student_id);
                continue;
            }
            const locale = row.locale === "HINDI" ? "hi" : "en";
            const expName = "3_days_trial_experiment";
            const flagrResp = await getFlagrResponseBool(row.student_id, expName);
            console.log(flagrResp);
            if (flagrResp && parseInt(versionCode) >= 914) {
                console.log("test");
                const trialDuration = 3;
                const studentId = row.student_id;
                const packageId = row.id;
                const courseDisplayName = row.display_name;
                const variantId = row.variant_id;
                const result = await createSubscriptionEntryForTrial(
                    studentId,
                    variantId,
                    packageId,
                    -1,
                    trialDuration,
                );
                if (!_.isEmpty(result)) {
                    if (!_.isEmpty(row.gcm_reg_id)) {
                        const notificationPayload = {
                            event: "course_details",
                            image: row.demo_video_thumbnail || "",
                            title: templates.notification[locale].title,
                            message: templates.notification[locale].message.replace(
                                "{{1}}",
                                courseDisplayName,
                            ),
                            firebase_eventtag: templates.notification[locale].firebaseTag,
                            s_n_id: templates.notification[locale].firebaseTag,
                        };
                        if (parseInt(row.parent) === 4) {
                            notificationPayload.event = "course_category";
                            notificationPayload.data = JSON.stringify({
                                category_id: "Kota Classes",
                            });
                        } else {
                            notificationPayload.data = JSON.stringify({
                                id: row.assortment_id,
                            });
                        }
                        kafka.sendNotification([row.student_id], [row.gcm_reg_id], notificationPayload);
                        sentUsers.push(row.student_id);
                    }
                }
            }
        }
        console.log(sentUsers);
        console.log(`the script successfully ran at ${new Date()}`);
        job.progress(100);
        return {
            data: {
                done: true,
            },
        };
    } catch (err) {
        console.log(err);
        return {
            err,
        };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "*/30 * * * *", // * at every 30 minutes
};
