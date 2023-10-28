// This cron deactivates the active subscriptions which has crossed validity
const {
    mysql, sendgridMail, slack, config,
} = require("../../modules");

async function setPackageInactive() {
    const sql = "update student_package_subscription set is_active = 0 where end_date < CURRENT_DATE and is_active = 1";
    return mysql.writePool.query(sql).then((res) => res[0]);
}

async function start(job) {
    const fromEmail = "autobot@doubtnut.com";
    const toEmail = "tech-alerts@doubtnut.com";
    const ccList = ["dipankar@doubtnut.com", "prashant.gupta@doubtnut.com"];
    const blockNew = [];
    try {
        await setPackageInactive();
        await sendgridMail.sendMail(fromEmail, toEmail, "CRON | VIP expire cron ran successfully", "All the expired active subscriptions have been deactivated", [], ccList);
        blockNew.push({
            type: "section",
            text: { type: "mrkdwn", text: "CRON | VIP expire cron ran successfully" },
        });
        await slack.sendMessage("#payments-team-dev", blockNew, config.paymentsAutobotSlackAuth);

        return { err: null, data: null };
    } catch (e) {
        await sendgridMail.sendMail(fromEmail, toEmail, "CRON | ALERT!!! Exception in expiring VIP", JSON.stringify(e), [], ccList);
        blockNew.push({
            type: "section",
            text: { type: "mrkdwn", text: `CRON | ALERT!!! Exception in expiring VIP <@U01MJU54A21> <@U0273ABLEPL> <@ULGN432HL>:\n\`\`\`${e.stack}\`\`\`` },
        });
        await slack.sendMessage("#payments-team-dev", blockNew, config.paymentsAutobotSlackAuth);
        return { err: e, data: null };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "30 00 * * *",
};
