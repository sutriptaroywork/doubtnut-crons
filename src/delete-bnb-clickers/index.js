const { mysql } = require("../../modules");

async function deleteExpiredBNBClickers() { // * Around 100-150ms
    const sql = "delete from bnb_clickers where is_active = 1 and is_sent = 1 and created_at < date_sub(CURRENT_TIMESTAMP, INTERVAL 2 HOUR)";
    return mysql.writePool.query(sql).then((res) => res[0]);
}

async function start(job) {
    try {
        const bnbClickers = await deleteExpiredBNBClickers();
        console.log(`Affected rows ${bnbClickers.affectedRows}`);
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
    cron: "0 */2 * * *",
};
