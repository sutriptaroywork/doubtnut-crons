/* eslint-disable no-await-in-loop */
const moment = require("moment");
const { getMongoClient } = require("../../modules");

const db = "studentdb";

const events = "otp_events";
const countSummaryHourly = "otp_count_summary_hourly";
const verificationDelaySummaryHourly = "otp_verification_delay_summary_hourly";
const dlrDelaySummaryHourly = "otp_dlr_delay_summary_hourly";

function createIndex(client) {
    client.collection(countSummaryHourly).createIndexes([
        { key: { date: 1, hr: 1, status: 1 }, unique: true },
        { key: { createdAt: 1 }, expireAfterSeconds: 7776000 },
    ]);
    client.collection(verificationDelaySummaryHourly).createIndexes([
        { key: { date: 1, hr: 1, v: 1 }, unique: true },
        { key: { createdAt: 1 }, expireAfterSeconds: 7776000 },
    ]);
    client.collection(dlrDelaySummaryHourly).createIndexes([
        {
            key: {
                date: 1, hr: 1, service: 1, status: 1, verificationStatus: 1,
            },
            unique: true,
        },
        { key: { createdAt: 1 }, expireAfterSeconds: 7776000 },
    ]);
}

async function count(client) {
    const lastRun = await client.collection(countSummaryHourly)
        .find()
        .sort({ _id: -1 })
        .limit(1)
        .toArray();
    const s = (lastRun.length ? moment(`${lastRun[0].date}T${lastRun[0].hr}:00:00`) : moment().subtract(1, "d").startOf("h")).toDate();
    const e = moment().startOf("h").toDate();
    const data = await client.collection(events).aggregate([{
        $match: {
            createdAt: {
                $gt: s,
                $lte: e,
            },
        },
    }, {
        $project: {
            date: { $dateToString: { format: "%Y-%m-%d", date: "$createdAt" } },
            hr: { $dateToString: { format: "%H", date: "$createdAt" } },
            verifiedAt: 1,
            createdAt: 1,
            verificationStatus: 1,
            __v: 1,
        },
    }, {
        $group: {
            _id: {
                date: "$date",
                hr: "$hr",
                status: "$verificationStatus",
                v: { $cond: [{ $gte: ["$__v", 5] }, 5, "$__v"] },
            },
            c: { $sum: 1 },
        },
    }, {
        $project: {
            _id: 0,
            date: "$_id.date",
            hr: "$_id.hr",
            v: "$_id.v",
            status: { $ifNull: ["$_id.status", "UNVERIFIED"] },
            count: "$c",
            createdAt: new Date(),
        },
    }, {
        $sort: { date: 1, hr: 1 },
    }])
        .maxTimeMS(1800000)
        .toArray();
    try {
        await client.collection(countSummaryHourly).insertMany(data, { ordered: false });
    } catch (err) {
        console.error(err);
    }
}

async function verificationDelay(client) {
    const lastRun = await client.collection(verificationDelaySummaryHourly)
        .find()
        .sort({ _id: -1 })
        .limit(1);
    const s = (lastRun.length ? moment(`${lastRun[0].date}T${lastRun[0].hr}:00:00`) : moment().subtract(1, "d").startOf("h")).toDate();
    const e = moment().startOf("h").toDate();
    const data = await client.collection(events).aggregate([{
        $match: {
            createdAt: {
                $gt: s,
                $lte: e,
            },
            verificationStatus: "VERIFIED",
        },
    }, {
        $project: {
            date: { $dateToString: { format: "%Y-%m-%d", date: "$createdAt" } },
            hr: { $dateToString: { format: "%H", date: "$createdAt" } },
            delay: { $subtract: ["$verifiedAt", "$createdAt"] },
            verifiedAt: 1,
            createdAt: 1,
            verificationStatus: 1,
            v: { $cond: { if: { $gte: [{ $size: "$services" }, 5] }, then: 5, else: { $size: "$services" } } },
        },
    }, {
        $group: {
            _id: { date: "$date", hr: "$hr", v: "$v" },
            delay: { $avg: "$delay" },
            c: { $sum: 1 },
        },
    }, {
        $project: {
            _id: 0,
            date: "$_id.date",
            hr: "$_id.hr",
            v: "$_id.v",
            delay: {
                $divide: ["$delay", 1000],
            },
            createdAt: new Date(),
        },
    }, {
        $sort: {
            date: 1, hr: 1, v: 1,
        },
    }])
        .maxTimeMS(1800000)
        .toArray();
    data.forEach((x) => {
        x.delay = x.delay.toFixed(2);
    });
    try {
        await client.collection(verificationDelaySummaryHourly).insertMany(data, { ordered: false });
    } catch (err) {
        console.error(err);
    }
}

async function dlrDelay(client) {
    const lastRun = await client.collection(dlrDelaySummaryHourly)
        .find()
        .sort({ _id: -1 })
        .limit(1)
        .toArray();
    const s = (lastRun.length ? moment(`${lastRun[0].date}T${lastRun[0].hr}:00:00`) : moment().subtract(1, "d").startOf("h")).toDate();
    const e = moment().subtract(6, "h").startOf("h").toDate();
    const data = await client.collection(events).aggregate([{
        $match: {
            createdAt: {
                $gt: s,
                $lte: e,
            },
        },
    }, {
        $unwind: "$services",
    }, {
        $project: {
            service: "$services.service",
            status: "$services.dlr",
            date: { $dateToString: { format: "%Y-%m-%d", date: "$createdAt" } },
            hr: { $dateToString: { format: "%H", date: "$createdAt" } },
            delay: { $subtract: ["$services.dlrAt", "$services.createdAt"] },
            verificationStatus: { $ifNull: ["$verificationStatus", "UNVERIFIED"] },
        },
    }, {
        $group: {
            _id: {
                date: "$date", hr: "$hr", service: "$service", status: "$status", verificationStatus: "$verificationStatus",
            },
            delay: { $avg: "$delay" },
        },
    }, {
        $project: {
            _id: 0,
            date: "$_id.date",
            hr: "$_id.hr",
            service: "$_id.service",
            status: "$_id.status",
            verificationStatus: "$_id.verificationStatus",
            delay: {
                $divide: ["$delay", 1000],
            },
            createdAt: new Date(),
        },
    }, {
        $sort: {
            date: 1, hr: 1, status: 1, verificationStatus: 1,
        },
    }])
        .maxTimeMS(1800000)
        .toArray();

    data.forEach((x) => {
        x.delay = x.delay ? x.delay.toFixed(2) : null;
    });
    try {
        await client.collection(dlrDelaySummaryHourly).insertMany(data, { ordered: false });
    } catch (err) {
        console.error(err);
    }
}

async function start(job) {
    // OTP Send Count
    // OTP Requested Count
    // OTP Failure by Service %
    // OTP Delivered Count by Service %
    // Avg Time Delivery by Service for 1st OTP
    // Avg Time Delivery by Service for 2nd OTP
    // Avg Time Delivery by Service for 3rd OTP
    // Avg Latency by Service
    // Avg number of OTP for successful login/ user login via OTP
    // %Users who login via 1st OTP
    // %Users who login via 2nd OTP
    // %Users who login via 3rd OTP
    // %Users who login via 4th OTP

    const client = (await getMongoClient()).db(db);
    createIndex(client);
    const funcArr = [
        count,
        verificationDelay,
        dlrDelay,
    ];
    for (let i = 0; i < funcArr.length; i++) {
        const func = funcArr[i];
        await func(client);
        await job.progress(parseInt(100 / funcArr.length));
    }
    await job.progress(100);
    return { data: funcArr };
}

module.exports.start = start;
module.exports.opts = {
    cron: "10 * * * *",
};
