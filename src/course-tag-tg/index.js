const moment = require("moment");
const { redshift, mysql, redis: redisClient } = require("../../modules");

// async function tgSql() {
//     const resultArray = [];
//     let i = 1;
//     while (true) {
//         const sql = `select ss.student_id,ss.gcm_reg_id,ss.app_version from (select distinct s.student_id as student_id ,gcm_reg_id,app_version from classzoo1.students s where locale in('en','hi') and s.gcm_reg_id is not null and s.app_version is not null and s.student_class in('6')) as ss left join (select distinct m.student_id from (select * from classzoo1.course_details where assortment_type='course' and is_active='1' and is_free='0') as a join (select * from classzoo1.package) as b on a.assortment_id=b.assortment_id join ( select * from classzoo1.payment_summary where master_subscription_start_date < CURRENT_DATE and master_subscription_end_date > CURRENT_DATE and next_package_id is null) as m on b.id=m.new_package_id)as x on ss.student_id=x.student_id where x.student_id is null LIMIT 1000000 OFFSET ${(i - 1) * 1000000}`;
//         const result = await redshift.query(sql);
//         if (result.length === 0) {
//             break;
//         }
//         i++;
//         resultArray.push(JSON.stringify(result));
//     }
//     return resultArray;
// }

async function getDetails(query) {
    if (query[query.length - 1] === ";") {
        query = query.slice(0, query.length - 1);
    }
    const resultArray = [];
    let i = 1;
    while (true) {
        const sql = `${query} LIMIT 1000000 OFFSET ${(i - 1) * 1000000}`;
        const result = await redshift.query(sql);
        if (result.length === 0) {
            break;
        }
        i++;
        resultArray.push(JSON.stringify(result));
    }
    return resultArray;
}

async function getAllTargetGroups() {
    const sql = "select target_group from course_tags where is_active = 1 group by target_group";
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getAllTargetGroupQueries(targetGroups) {
    const sql = `select * from target_group where id in (${targetGroups.join(",")})`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

// async function streamSQL() {
//     const { Readable } = stream;
//     const res1 = {};
//     let i = 1;
//     const s = new Readable({
//         async read(size) {
//             const sql = `select ss.student_id,ss.gcm_reg_id,ss.app_version from (select distinct s.student_id as student_id ,gcm_reg_id,app_version from classzoo1.students s where locale in('en','hi') and s.gcm_reg_id is not null and s.app_version is not null and s.student_class in('6')) as ss left join (select distinct m.student_id from (select * from classzoo1.course_details where assortment_type='course' and is_active='1' and is_free='0') as a join (select * from classzoo1.package) as b on a.assortment_id=b.assortment_id join ( select * from classzoo1.payment_summary where master_subscription_start_date < CURRENT_DATE and master_subscription_end_date > CURRENT_DATE and next_package_id is null) as m on b.id=m.new_package_id)as x on ss.student_id=x.student_id where x.student_id is null LIMIT 1000000 OFFSET ${(i - 1) * 1000000}`;
//             const result = await redshift.query(sql);
//             if (result.length === 0) {
//                 this.push(null);
//             } else {
//                 this.push(JSON.stringify(result));
//             }
//             i++;
//         },
//     });
//     const res = await s._read();
//     console.log(res);
//     s.pipe(res1);
// }

async function updateTimestamp(id) {
    return redisClient
        .multi()
        .hset(`TARGET_GROUP_CRON:${id}`, "TIMESTAMP", JSON.stringify(moment().unix()))
        .expire(`TARGET_GROUP_CRON:${id}`, 60 * 60 * 26) // * 26 hours
        .execAsync();
}

async function storeInCache(key, field) {
    return redisClient.hsetAsync(key, field, 1);
}

async function processTargetGroups(job) {
    const targetGroupsResult = await getAllTargetGroups();
    const targetGroups = targetGroupsResult.map((item) => item.target_group).filter((item) => item);
    const targetGroupsSQL = await getAllTargetGroupQueries(targetGroups);
    for (let i = 0; i < targetGroupsSQL.length; i++) {
        const { id, sql } = targetGroupsSQL[i];
        updateTimestamp(id);
        const resultArray = await getDetails(sql);
        for (let resultArrayItr = 0; resultArrayItr < resultArray.length; resultArrayItr++) {
            const sqlArray = JSON.parse(resultArray[resultArrayItr]);
            for (let sqlArrayItr = 0; sqlArrayItr < sqlArray.length; sqlArrayItr++) {
                const key = `TARGET_GROUP_CRON:${id}`;
                const { student_id: field } = sqlArray[sqlArrayItr];
                storeInCache(key, field);
            }
        }
        job.progress(parseInt(((i + 1) / targetGroupsSQL.length) * 100));
    }
    console.log(targetGroupsSQL);
}

async function start(job) {
    // try {
    // const res = await tgSql();
    // const res1 = await tgSql();
    // console.log(res.length);
    // console.log(res1.length);
    await processTargetGroups(job);
    job.progress(100);
    return {
        data: {
            done: true,
        },
    };
}
module.exports.start = start;
module.exports.opts = {
    cron: "40 2 * * *", // * 2:40 AM Every day
};
