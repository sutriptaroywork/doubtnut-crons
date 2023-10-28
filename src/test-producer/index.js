const kafka = require("../../modules/kafka");

async function start() {
    await kafka.publishRaw("test", { data: 123, to: "abc", studentId: 1 });
    await new Promise((resolve) => {
        setTimeout(resolve, 600000);
    });
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 * * * *",
    removeOnComplete: 7,
    removeOnFail: 7,
};
