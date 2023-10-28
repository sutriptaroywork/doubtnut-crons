/* eslint-disable no-await-in-loop */

/* minVersionAllowed = 885;
minAppVersion = "7.8.237";
v2minVersionAllowed = 906;
v2minAppVersion = "7.8.258"; */
const moment = require("moment");
const _ = require("lodash");
const { redshift } = require("../../modules");
const mysql = require("../../modules/mysql");
const { getMongoClient } = require("../../modules");

const queryInterval = 24;
const activeMembersCollection = "p2p_active_members";
// const inactiveMembersCollection = "p2p_inactive_members";
const db = "doubtnut";

async function getUninstalledUsers() {
    const sql = "SELECT userid as student_id from analytics.uninstall_dump_data where date >= current_date - 2 and userid is not null";
    console.log(sql);
    const users = await redshift.query(sql).then((res) => res);
    const studentsIds = [];
    for (let i = 0; i < users.length; i++) {
        if (users[i] && users[i].student_id) {
            studentsIds.push(parseInt(users[i].student_id));
        }
    }
    // console.log(users.length, " total users");
    return studentsIds;
}

async function insertBatch(client, documents) {
    // Inserting documents in new collection
    const bulkInsert = await client.collection("p2p_inactive_members").initializeUnorderedBulkOp();
    const insertedIds = [];
    let id;
    documents.forEach((doc) => {
        id = doc._id;
        // Insert without raising an error for duplicates
        bulkInsert.find({ _id: id }).upsert().replaceOne(doc);
        insertedIds.push(id);
    });
    await bulkInsert.execute();
    return insertedIds;
}

async function deleteBatch(client, documents) {
    // Deleting documents from old collection
    const bulkRemove = await client.collection("p2p_active_members").initializeUnorderedBulkOp();
    documents.forEach((doc) => {
        bulkRemove.find({ _id: doc._id }).deleteOne();
    });
    await bulkRemove.execute();
    return true;
}

async function moveDocuments(client, filter, batchSize) {
    // console.log(`Moving ${await client.collection("p2p_active_members").find(filter).count()} documents from ${sourceCollection} to ${targetCollection}`);
    while (await client.collection("p2p_active_members").find(filter).count()) {
        const sourceDocs = await client.collection("p2p_active_members").find(filter).limit(batchSize).toArray();
        // inserting documents to new collection
        const idsOfCopiedDocs = await insertBatch(client, sourceDocs);

        const targetDocs = await client.collection("p2p_inactive_members").find({ _id: { $in: idsOfCopiedDocs } }).toArray();
        // removing documents from old collection
        await deleteBatch(client, targetDocs);
    }
    return true;
}

async function archiveUninstalledUsers(client, job) {
    const uninstalledStudents = await getUninstalledUsers();
    job.progress(70);
    if (uninstalledStudents.length) {
        const chunk = 50000;
        for (let i = 0; i < uninstalledStudents.length; i += chunk) {
            const studentData = uninstalledStudents.slice(i, i + chunk);
            const filter = {
                student_id: { $in: studentData },
            };
            await moveDocuments(client, filter, 10000);
        }
    }
    job.progress(90);
    return true;
}

function getNewStudents(startTime, endTime) {
    const sql = `SELECT student_id, student_class, locale, is_online as version_code FROM students WHERE 
                 updated_at BETWEEN '${startTime}' AND '${endTime}'`;
    console.log(sql);
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function start(job) {
    const mongoClient = (await getMongoClient()).db(db);
    const startTime = moment().subtract(queryInterval, "hours").format("YYYY-MM-DD HH:mm:ss");
    const endTime = moment().format("YYYY-MM-DD HH:mm:ss");
    job.progress(10);
    const newStudents = await getNewStudents(startTime, endTime);
    console.log("fetched new students", newStudents.length);
    job.progress(30);
    // adding new users which have installed app in between last 12 hours to 36hours
    for (let k = 0; k <= newStudents.length; k++) {
        if (newStudents[k] && newStudents[k].student_id && !_.isNull(newStudents[k].version_code)) {
            const studentClass = (newStudents[k].student_class ? parseInt(newStudents[k].student_class) : 0);
            const studentLocale = (newStudents[k].locale ? newStudents[k].locale : null);
            console.log('executing mongo query');
            await mongoClient.collection(activeMembersCollection).updateOne(
                { student_id: newStudents[k].student_id },
                {
                    $set: {
                        updated_at: new Date(),
                        is_active: true,
                        student_class: studentClass,
                        notification_sent_count: 0,
                        locale: studentLocale,
                        version_code: newStudents[k].version_code,
                    },
                },
                { upsert: true }, // upsert to create a new doc if none exists and new to return the new, updated document instead of the old one.
                (err, doc) => {
                    if (err) {
                        console.error("Something wrong when updating data!");
                    }
                    console.log(doc);
                },
            );
            console.log('executed query');
            await new Promise((resolve) => {
                console.log("Waiting for 200 ms....");
                setTimeout(resolve, 200);
            });
            job.progress(40);
        }
    }
    job.progress(50);
    await archiveUninstalledUsers(mongoClient, job);
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "08 22 * * *", // At 03:38 AM everyday
};
