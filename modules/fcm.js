const _ = require("lodash");
const request = require("request");
const config = require("./config");

async function sendFcm(student_id, fcmId, message, type = null, admin = null) {
    try {
        // See documentation on defining a message payload.
        if (!_.isNull(fcmId)) {
            const user = [{ id: student_id, gcmId: fcmId }];
            if (!("firebase_eventtag" in message) || message.firebase_eventtag == "") message.firebase_eventtag = "user_journey";
            const options = {
                method: "POST",
                url: config.newtonUrl,
                headers:
                    { "Content-Type": "application/json" },
                body:
                    { notificationInfo: message, user },
                json: true,
            };
            return new Promise((res, rej) => {
                request(options, (error, response, body) => {
                    if (error) {
                        console.log(error);
                        rej(error);
                    }
                    console.log(body);
                    res(body);
                });
            });
        }
    } catch (error) {
        console.log(error);
    }
}

module.exports = {
    sendFcm,
};
