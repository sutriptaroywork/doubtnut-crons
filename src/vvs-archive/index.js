/* eslint-disable no-await-in-loop */
const moment = require("moment");
const { config, mysql, getMongoClient } = require("../../modules");

const db = "doubtnut";
const vvsArchive = "vvs_archive_logs";

const batchSize = parseInt(process.env.VVS_BATCH_SIZE) || 1000;

async function getMaxViewIdFromArchive() {
    const sql = "select max(view_id) as view_id from video_view_stats_archive";
    const { view_id } = await mysql.pool.query(sql).then((res) => res[0][0]);
    return view_id || 0;
}

async function getMinViewId() {
    const sql = "select min(view_id) as view_id from video_view_stats";
    const { view_id } = await mysql.pool.query(sql).then((res) => res[0][0]);
    return view_id;
}

async function getMaxViewIdToCopy(id) {
    const sql = `select view_id, created_at from video_view_stats where view_id > ${id} order by view_id limit 1 offset ${batchSize}`;
    console.log(sql);
    const { view_id, created_at } = await mysql.pool.query(sql).then((res) => res[0][0]);
    console.log(view_id, created_at.toISOString());
    if (moment().add("5:30").diff(moment(created_at), "minute") > 1 * 24 * 60) {
        return view_id;
    }
}

async function getMinViewIdToDelete(id) {
    const sql = `select view_id, created_at from video_view_stats where view_id > ${id} order by view_id limit 1 offset ${batchSize}`;
    console.log(sql);
    const { view_id, created_at } = await mysql.pool.query(sql).then((res) => res[0][0]);
    console.log(view_id, created_at.toISOString());
    if (moment().add("5:30").diff(moment(created_at), "minute") > 30 * 24 * 60) {
        return view_id;
    }
}

function getCopyToArchiveQuery(id) {
    return `insert ignore into video_view_stats_archive select * from video_view_stats where view_id < ${id}`;
}

function getDeleteFromSourceQuery(id) {
    return `delete from video_view_stats where view_id < ${id}`;
}

async function checkConsistency(maxId, minId, sql, insertRes, delRes) {
    const client = (await getMongoClient()).db(db);

    const insertInfoObj = {};

    const insertInfo = insertRes.info.replace(/: /g, ":").split("  ");

    insertInfo.forEach((x) => {
        const a = x.split(":");
        insertInfoObj[a[0]] = parseInt(a[1]);
    });

    await client.collection(vvsArchive).insertOne({
        maxId, minId, insertRes, insertInfoObj, delRes, sql,
    }, { ordered: false });

    if ((insertInfoObj.Duplicates + insertRes.affectedRows) !== batchSize) {
        console.error(insertRes);
        throw new Error("Insert data inconsistent");
    }
    if (delRes.affectedRows !== batchSize) {
        console.error(delRes);
        throw new Error("Delete data inconsistent");
    }
}

async function moveData(maxId, minId) {
    const sql1 = getCopyToArchiveQuery(maxId);
    const sql2 = getDeleteFromSourceQuery(minId);
    const sql = `${sql1}; ${sql2}`;
    console.log(sql);
    const con = await mysql.writePool.getConnection();
    try {
        await con.query("START TRANSACTION");
        const res1 = await con.query(sql1);
        const res2 = await con.query(sql2);

        await checkConsistency(maxId, minId, sql, res1[0], res2[0]);

        await con.query("COMMIT");
        con.release();
    } catch (e) {
        console.error(e);
        await con.query("ROLLBACK");
        con.release();
        throw e;
    }
}

async function start(job) {
    const maxArchiveId = await getMaxViewIdFromArchive();
    job.progress(20);

    const minVVSId = await getMinViewId();
    const baseId = Math.min(maxArchiveId, minVVSId - 1);
    job.progress(40);

    const maxId = await getMaxViewIdToCopy(baseId);
    if (!maxId) {
        return { err: new Error("MaxId is to copy null") };
    }
    job.progress(60);

    const minId = await getMinViewIdToDelete(baseId);
    if (!minId) {
        return { err: new Error("MinId to delete is null") };
    }
    job.progress(80);
    await moveData(maxId, minId);
    console.log("Done", minId, maxId);
    return { data: { minId, maxId } };
}

module.exports.start = start;
module.exports.opts = {
    cron: "*/40 * 22,23,0-5 * * *",
    removeOnComplete: 10,
    removeOnFail: 20,
};
