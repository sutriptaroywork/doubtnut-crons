/* eslint-disable guard-for-in */
/* eslint-disable no-await-in-loop */
const moment = require("moment");
const {
    redis,
} = require("../../modules");

// const redisPattern = ["branch_data"];

const scanAll = async (hashName, pattern) => {
    const found = [];
    let cursor = "0";
    do {
        const reply = await redis.hscanAsync(hashName, cursor, "MATCH", pattern, "COUNT", "1000");
        console.log(reply);
        cursor = reply[0];
        found.push(...reply[1]);
    } while (cursor !== "0");
    return found;
};
async function start(job) {
    try {
        const data = await scanAll("branch_data", "*");
        console.log("data");
        console.log(data.length);
        for (let i = 0; i < data.length; i++) {
            // console.log(data[i + 1]);
            if (i % 2 === 0) {
                const campaignDate = data[i + 1].split(":_:")[1];
                // expire 1 month old keys
                if (moment(campaignDate).isBefore(moment().subtract(1, "month"))) {
                    // delete key
                    redis.hdelAsync("branch_data", data[i]);
                }
            }
        }
        job.progress(100);
        console.log(`the script successfully ran at ${new Date()}`);
        return {
            data: {
                done: true,
            },
        };
    } catch (err) {
        console.log(err);
        return { err };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "13 * * * *",
};
