const { mysql } = require("../../modules");

function getYesterdayPayments() {
    const sql = "SELECT a.*, b.assortment_id from (SELECT * from payment_summary where created_at >= DATE_ADD(CURDATE(), INTERVAL -1 DAY) and new_package_id is not null and is_renewed is null) as a left join package as b on a.new_package_id=b.id where b.assortment_id is not null";
    return mysql.pool.query(sql).then((res) => res[0]);
}

function getPreviousPackagesWithAssortmentIdUsingStudentId(studentId, id) {
    const sql = `SELECT a.*, b.assortment_id from (SELECT * from payment_summary where student_id='${studentId}' and id< '${id}' and new_package_id is not null and master_subscription_start_date <= CURDATE() and master_subscription_end_date >= CURDATE()) as a left join package as b on a.new_package_id=b.id where b.assortment_id is not null`;
    console.log(sql);
    return mysql.pool.query(sql).then((res) => res[0]);
}

function updateRenewed(id, isRenewed) {
    const sql = `update payment_summary SET is_renewed='${isRenewed}' where id='${id}'`;
    return mysql.writePool.query(sql).then((res) => res[0]);
}

async function start(job) {
    try {
        const payments = await getYesterdayPayments();
        for (let i = 0; i < payments.length; i++) {
            let isRenewed = 0;
            const prevPackages = await getPreviousPackagesWithAssortmentIdUsingStudentId(payments[i].student_id, payments[i].id);
            if (prevPackages.length == 0) {
                // update row is isRenewed false
                await updateRenewed(payments[i].id, isRenewed);
            } else {
                // iterate and check
                for (let j = 0; j < prevPackages.length; j++) {
                    if (prevPackages[j].assortment_id == payments[i].assortment_id) {
                        isRenewed = 1;
                        break;
                    }
                }
                await updateRenewed(payments[i].id, isRenewed);
            }
        }
        return { err: null, data: null };
    } catch (err) {
        console.error(err);
        return { err };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 3 * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
