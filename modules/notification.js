const request = require("request");
const config = require("./config");

async function sendNotification(user, notificationInfo) {
    console.log("notificationInfo");
    console.log(notificationInfo);
    console.log(user);
    const options = {
        method: "POST",
        url: config.newtonUrl,
        headers:
            { "Content-Type": "application/json" },
        body:
            { notificationInfo, user },
        json: true,
    };

    // console.log(options);
    return new Promise((resolve, reject) => {
        try {
            request(options, (error, response, body) => {
                if (error) console.log(error);
                console.log(body);
                resolve();
            });
        } catch (err) {
            // fn(err);
            console.log(err);
            reject(err);
        }
    });
}
module.exports = {
    sendNotification,
};
