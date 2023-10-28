const moment = require("moment");
const redis = require("../../modules/redis");
const {
    mysql, config, kafka, slack,
} = require("../../modules/index");

async function getM1(startTime, endTime) {
    const sql = `select
	srd2.invitor_student_id,
	max(id)
from
	student_referral_disbursement srd2
left join (
	select
		srd.invitor_student_id,
		count(srd.amount) as number_of_sales
	from
		student_referral_disbursement srd
	where
		srd.amount in (1000, 0)
		and srd.id >= 1833
	group by
		1
	order by
		2 desc ) as a 
on
	srd2.invitor_student_id = a.invitor_student_id
where
	srd2.amount in (1000, 0)
	and a.number_of_sales = 3
	and srd2.created_at > ? and srd2.created_at <= ? 
group by
	srd2.invitor_student_id`;
    return mysql.pool.query(sql, [startTime, endTime]).then((x) => x[0]);
}

// getM2

async function getM2(startTime, endTime) {
    const sql = `select
	srd2.invitor_student_id,
	max(id) as id
from
	student_referral_disbursement srd2
left join 
(
	select
		srd.invitor_student_id,
		count(srd.amount) as number_of_sales
	from
		student_referral_disbursement srd
	where
		srd.amount in (1000, 0)
		and srd.id >= 1833
	group by
		1
	order by
		2 desc ) as a 
on
	srd2.invitor_student_id = a.invitor_student_id
where
	srd2.amount in (1000, 0)
	and a.number_of_sales = 6
	and srd2.created_at > ? and srd2.created_at <= ? 
group by
	srd2.invitor_student_id`;
    return mysql.pool.query(sql, [startTime, endTime]).then((x) => x[0]);
}

async function getM3(startTime, endTime) {
    const sql = `select
	srd2.invitor_student_id,
	max(id) as id
from
	student_referral_disbursement srd2
left join (
	select
		srd.invitor_student_id,
		count(srd.amount) as number_of_sales
	from
		student_referral_disbursement srd
	where
		srd.amount in (1000, 0)
		and srd.id >= 1833
	group by
		1
	order by
		2 desc ) as a
on
	srd2.invitor_student_id = a.invitor_student_id
where
	srd2.amount in (1000, 0)
	and a.number_of_sales = 10
	and srd2.created_at > ? and srd2.created_at <= ? 
    group by
    srd2.invitor_student_id`;
    return mysql.pool.query(sql, [startTime, endTime]).then((x) => x[0]);
}

const templates = {
    notification: {
        M1: {
            title: "Badhai ho! Doubtnut se aapko mil raha hai Boat Airdopes ka inaam! 🎧",
            message: "Apna inaam lene ke liye click karo aur apna address bhar lo.",
            firebaseTag: "CEO_REFERRAL_M1",
        },
        M2: {
            title: "Badhai ho! Doubtnut se aapko mil raha hai Bluetooth Speakers ka inaam! 🎧",
            message: "Apna inaam lene ke liye click karo aur apna address bhar lo.",
            firebaseTag: "CEO_REFERRAL_M2",
        },
        M3: {
            title: "Badhai ho! Doubtnut se aapko mil raha hai Redmi 9 Phone ka inaam! 🎧",
            message: "Apna inaam lene ke liye click karo aur apna address bhar lo.",
            firebaseTag: "CEO_REFERRAL_M3",
        },
    },
};

async function sendNotification(studentId, gcmId, notificationTemplate, spdid) {
    try {
        const notificationPayload = {
            event: "submit_address_dialog",
            title: notificationTemplate.title,
            message: notificationTemplate.message,
            firebase_eventtag: notificationTemplate.firebaseTag,
            s_n_id: notificationTemplate.firebaseTag,
        };
        if (gcmId) {
            notificationPayload.data = JSON.stringify({
                type: "referral_v2_goodie",
                id: spdid,
            });

            await kafka.sendNotification(
                [studentId], [gcmId],
                notificationPayload,
            );
        }
    } catch (err) {
        console.log(err);
    }
}

async function setEndTimeStamp(timestamp) {
    return redis.setAsync("crwncrontime", timestamp);
}

async function getStartTimeStamp() {
    return redis.getAsync("crwncrontime");
}

async function getStudentData(sid) {
    const sql = "select * from students where student_id = ?";
    return mysql.pool.query(sql, [sid]).then((x) => x[0]);
}

async function start(job) {
    try {
        const now = moment().add(5, "hours").add(30, "minutes");
        const endTime = now.format("YYYY-MM-DD HH:mm:ss");

        let startTime;
        startTime = await getStartTimeStamp();
        if (!startTime) {
            startTime = now.subtract(15, "minutes").format("YYYY-MM-DD HH:mm:ss");
        }
        // startTime = "2022-07-15 15:48:37";
        setEndTimeStamp(endTime);
        const promises = [getM1(startTime, endTime), getM2(startTime, endTime), getM3(startTime, endTime)];
        const results = await Promise.all(promises);
        const m1 = results[0];
        const promises2 = [];
        const counts = {
            M1: 0,
            M2: 0,
            M3: 0,
        };
        const m1Promises = m1.map(async (x) => {
            const studentData = await getStudentData(x.invitor_student_id);
            const gcmId = studentData[0].gcm_reg_id;
            const spdid = x.id;
            if (gcmId) {
                counts.M1 += 1;
            }
            promises2.push(sendNotification(x.invitor_student_id, gcmId, templates.notification.M1, spdid));
        });
        await Promise.all(m1Promises);
        const m2 = results[1];
        const m2Promises = m2.map(async (x) => {
            const studentData = await getStudentData(x.invitor_student_id);
            const gcmId = studentData[0].gcm_reg_id;
            const spdid = x.id;
            if (gcmId) {
                counts.M2 += 1;
            }
            promises2.push(sendNotification(x.invitor_student_id, gcmId, templates.notification.M2, spdid));
        });
        await Promise.all(m2Promises);
        const m3 = results[2];
        const m3Promises = m3.map(async (x) => {
            const studentData = await getStudentData(x.invitor_student_id);
            const gcmId = studentData[0].gcm_reg_id;
            const spdid = x.id;
            if (gcmId) {
                counts.M3 += 1;
            }
            promises2.push(sendNotification(x.invitor_student_id, gcmId, templates.notification.M3, spdid));
        });
        // await sendNotification(156686578, "ey_OOuIRSKyO5_Xdizovyu:APA91bFx34EPI-xLM_nbGKbi_wqiI0O029lF1YhFwnw4snu0DzoBBoykN4aBPhs0MzmbvWc5FQR0R6181_Amr7EtedvBraIrm_-CopzuEEUnwGEbE59zry-cj2hOOZbVsY0BT8Vq1INx", templates.notification.M3, 500);
        await Promise.all(m3Promises);
        await Promise.all(promises2);

        const blocks = [];
        blocks.push({
            type: "section",
            text: { type: "mrkdwn", text: `*timestamps*: "${startTime}"- "${endTime}"` },
        }, {
            type: "section",
            text: { type: "mrkdwn", text: `*Boat Airdopes*: ${counts.M1}` },
        },
        {
            type: "section",
            text: { type: "mrkdwn", text: `*Bluetooth Speakers*: ${counts.M2}` },
        },
        {
            type: "section",
            text: { type: "mrkdwn", text: `*Redmi 9 Phone*:${counts.M3}` },
        });
        await slack.sendMessage("#ceo-referral-messages", blocks, "xoxb-534514142867-3817834587683-wTVDcewaHUjgOtkjCUkVElOS");
    } catch (e) {
        console.error(e);
    }
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "00 12 */3 * *", // At 12:00 every third day
};
