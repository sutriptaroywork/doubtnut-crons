const { CompressionTypes, Kafka } = require("kafkajs");
const config = require("./config");

// const topics = {
//     vvsUpdate: {
//         name: "vvs.update",
//         partition: 4,
//     },
//     newtonNotifications:
//         {
//             name: "PUSH_NOTIFICATION",
//             partition: 4,
//         },
// };

const kafka = new Kafka({
    clientId: "producer-api-server",
    brokers: config.kafkaHosts,
});

const producer = kafka.producer();
let connected = false;
producer.connect().then(() => {
    connected = true;
});

producer.on("producer.disconnect", () => {
    connected = false;
});

async function publishRaw(topic, kafkaMsgData) {
    try {
        const value = { meta: { studentId: kafkaMsgData.studentId, gcmId: kafkaMsgData.to, ts: Date.now() }, data: kafkaMsgData.data };
        if (!connected) {
            await producer.connect();
            connected = true;
        }
        await producer.send({
            topic,
            compression: CompressionTypes.GZIP,
            messages: [{
                value: JSON.stringify(value),
            }],
        });
    } catch (e) {
        console.error(e);
    }
}

// studentIds & gcmRegistrationIds should be passed as an array
async function sendNotification(studentIds, gcmRegistrationIds, notificationInfo) {
    function isValidNotificationInfo() {
        return "title" in notificationInfo && "message" in notificationInfo && "event" in notificationInfo && studentIds.length === gcmRegistrationIds.length;
    }

    if (!("data" in notificationInfo)) { notificationInfo.data = {}; }
    if (!("firebase_eventtag" in notificationInfo) || notificationInfo.firebase_eventtag === "") { notificationInfo.firebase_eventtag = "user_journey"; }

    if (!isValidNotificationInfo()) { return; }

    const chunk = 1000;
    for (let i = 0, j = studentIds.length; i < j; i += chunk) {
        const kafkaMsgData = {
            data: notificationInfo,
            to: gcmRegistrationIds.slice(i, i + chunk),
            studentId: studentIds.slice(i, i + chunk),
        };

        // eslint-disable-next-line no-await-in-loop
        await publishRaw("bull-cron.push.notification", kafkaMsgData);
    }
}

async function sendNotificationWithoutStudentIds(studentIds, gcmRegistrationIds, notificationInfo) {
    function isValidNotificationInfo() {
        return "title" in notificationInfo && "message" in notificationInfo && "event" in notificationInfo;
    }

    if (!("data" in notificationInfo)) { notificationInfo.data = {}; }
    if (!("firebase_eventtag" in notificationInfo) || notificationInfo.firebase_eventtag === "") { notificationInfo.firebase_eventtag = "user_journey"; }

    if (!isValidNotificationInfo()) { return; }

    const chunk = 1000;
    for (let i = 0, j = gcmRegistrationIds.length; i < j; i += chunk) {
        const kafkaMsgData = {
            data: notificationInfo,
            to: gcmRegistrationIds.slice(i, i + chunk),
            studentId: [],
        };

        // eslint-disable-next-line no-await-in-loop
        await publishRaw("bull-cron.push.notification", kafkaMsgData);
    }
}

module.exports = {
    sendNotification,
    publishRaw,
    sendNotificationWithoutStudentIds,
};
