/* eslint-disable no-await-in-loop */
const gc = require("expose-gc/function");
const Redis = require("ioredis");
const bluebird = require("bluebird");
const {
    redshift, mysql, sendgridMail, slack, config,
} = require("../../modules");
const redisClient = require("../../modules/redis");

async function getCachedTgs() {
    const sql = "select tg.id, tg.sql, tg.db_to_use from target_group tg where fetch_from_cache = 1 order by updated_at desc";
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function runMysqlQ(sql) {
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function setByKey(key, value) {
    return await redisClient.multi()
        .sadd(key, value)
        .expire(key, 24 * 60 * 60)
        .execAsync();
}

async function checkByKey(key) {
    return await redisClient.existsAsync(key);
}

async function deleteByKey(key) {
    return await redisClient.delAsync(key);
}

bluebird.promisifyAll(Redis);

async function start(job) {
    const fromEmail = "autobot@doubtnut.com";
    const toEmail = "prakher.gaushal@doubtnut.com";
    const ccList = ["dipankar@doubtnut.com", "prashant.gupta@doubtnut.com", "aditya.mishra@doubtnut.com"];
    const blockNew = [];
    try {
        const cachedTgs = await getCachedTgs();
        for (let i = 0; i < cachedTgs.length; i++) {
            try {
                const tgId = cachedTgs[i].id;
                let query;
                if (cachedTgs[i].sql) {
                    if (cachedTgs[i].sql[cachedTgs[i].sql.length - 1] === ";") {
                        cachedTgs[i].sql = cachedTgs[i].sql.slice(0, cachedTgs[i].sql.length - 1);
                    }
                    query = cachedTgs[i].sql.replace(/\n/g, " ").replace(/\r/g, "");
                }
                const countSql = `select count(*) as count from (${query}) as X`;
                let queryCount = [];
                if (cachedTgs[i].db_to_use == "redshift") {
                    queryCount = await redshift.query(countSql);
                } else if (cachedTgs[i].db_to_use == "mysql") {
                    queryCount = await runMysqlQ(countSql);
                }
                if (queryCount[0].count > 12000000) {
                    console.log("Query Limit Exceeded");
                    const loopBlockNew = [{
                        type: "section",
                        text: { type: "mrkdwn", text: `CRON | ALERT!!! Query limit exceeded in tg-redis-populate TG: ${cachedTgs[i].id}` },
                    }];
                    await slack.sendMessage("#payments-team-dev", loopBlockNew, config.paymentsAutobotSlackAuth);
                    continue;
                }
                const keyExists = await checkByKey(`target_group_${tgId}`);
                const resArr = [];
                let offsetParam = 1;
                // eslint-disable-next-line no-constant-condition
                while (true) {
                    // calling garbage collector for every loop
                    gc();
                    const sql = `select student_id from (${query}) tg_result ORDER BY tg_result.student_id DESC LIMIT 50000 OFFSET ${(offsetParam - 1) * 50000}`;
                    let studentDetails = [];
                    if (cachedTgs[i].db_to_use == "redshift") {
                        console.log(`redshift_${tgId}_${offsetParam}`);
                        studentDetails = await redshift.query(sql);
                    } else if (cachedTgs[i].db_to_use == "mysql") {
                        console.log(`mysql_${tgId}_${offsetParam}`);
                        studentDetails = await runMysqlQ(sql);
                    }
                    if (studentDetails.length === 0) {
                        break;
                    }
                    for (let j = 0; j < studentDetails.length; j++) {
                        resArr.push(studentDetails[j].student_id);
                    }
                    offsetParam++;
                }
                if (keyExists) {
                    console.log("Exists", `target_group_${tgId}`);
                    await deleteByKey(`target_group_${tgId}`);
                }
                while (resArr.length) {
                    await setByKey(`target_group_${tgId}`, resArr.splice(0, 100000));
                }
                console.log(resArr);
            } catch (e) {
                console.log(e);
                await sendgridMail.sendMail(fromEmail, toEmail, "CRON | ALERT!!! Exception in tg-redis-populate", JSON.stringify(e), [], ccList);
                const loopBlockNew = [{
                    type: "section",
                    text: { type: "mrkdwn", text: `CRON | ALERT!!! Exception in tg-redis-populate TG: ${cachedTgs[i].id}:\n\`\`\`${e.stack}\`\`\`` },
                }];
                await slack.sendMessage("#payments-team-dev", loopBlockNew, config.paymentsAutobotSlackAuth);
            }
        }
        return { err: null, data: null };
    } catch (e) {
        console.log(e);
        await sendgridMail.sendMail(fromEmail, toEmail, "CRON | ALERT!!! Exception in tg-redis-populate", JSON.stringify(e), [], ccList);
        blockNew.push({
            type: "section",
            text: { type: "mrkdwn", text: `CRON | ALERT!!! Exception in tg-redis-populate <@U01MJU54A21> <@U0273ABLEPL> <@ULGN432HL>:\n\`\`\`${e.stack}\`\`\`` },
        });
        await slack.sendMessage("#payments-team", blockNew, config.paymentsAutobotSlackAuth);
        return { e };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 */3 * * *",
};
