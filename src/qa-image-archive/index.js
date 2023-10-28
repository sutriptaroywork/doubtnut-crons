const fs = require("fs");
const archiver = require("archiver");
const _ = require("lodash");
const { mysql, aws, getMongoClient } = require("../../modules");

const db = "doubtnut";
const collection = "qa_image_archive_logs";
const batchsize = 10000;

let count = 0;

async function getLastArchivedQid() {
    const client = (await getMongoClient()).db(db);
    const doc = await client.collection(collection).find().sort({ _id: -1 }).limit(1)
        .toArray()
        .then((x) => x[0]);
    return doc || {
        lastId: 575998644,
    };
}

async function setLastArchivedQid(firstId, lastId) {
    const client = (await getMongoClient()).db(db);
    return client.collection(collection).insertOne({ firstId, lastId, createdAt: new Date() });
}

async function getQAImages() {
    const { lastId } = await getLastArchivedQid();
    console.log("lastId", lastId);
    // const sql = `select question_id, question_image from questions where student_id >100 and (question_image like 'upload%' or question_image like '8400%' or question_image like '600%' or question_image like 'bot%') and question_id>${lastId} and is_answered=0 and is_text_answered=0 order by question_id limit ${batchsize}`;
    const sql = `select question_id, question_image from questions_new where question_id>${lastId} order by question_id limit ${batchsize}`;
    const data = await mysql.pool.query(sql).then(([res]) => res);
    if (data.length < batchsize) {
        return;
    }
    return {
        firstId: lastId + 1,
        lastId: data[data.length - 1].question_id,
        images: _.uniq(data.map((x) => x.question_image)),
    };
}

async function downloadImage(Key) {
    console.log("\tDownloading", Key);
    try {
        const res = await aws.s3.getObject({
            Bucket: "doubtnut-static/images",
            Key,
        }).promise();
        console.log("\tDownloaded", Key);
        return res.Body;
    } catch (e) {
        console.error(e);
        console.error("Unable to download", Key);
    }
}

async function deleteImages(images) {
    if (!images.length) {
        return;
    }
    while (images.length) {
        const batch = images.splice(0, 1000);
        console.log("Deleting images", batch.length);
        try {
            // eslint-disable-next-line no-await-in-loop
            const res = await aws.s3.deleteObjects({
                Bucket: "doubtnut-static",
                Delete: {
                    Objects: batch.filter(Boolean).map((Key) => ({ Key: `images/${Key}` })),
                },
            }).promise();
            console.log(res.$response.data.Deleted);
            console.log("Deleted images from s3");
        } catch (e) {
            console.error(e);
            console.error("Unable to delete images from s3");
            throw new Error("Unable to delete images from s3");
        }
    }
}

async function processImage(Key, archive) {
    if (!Key) {
        return;
    }
    const buf = await downloadImage(Key);
    if (!buf) {
        return;
    }
    try {
        console.log("\tAdding to archive", Key);
        archive.append(buf, { name: Key, store: true });
        count++;
        console.log(count, "\tAdded to archive", Key);
    } catch (e) {
        console.error(e);
    }
    return Key;
}

async function processImages(firstId, lastId, images) {
    const zip = `${__dirname}/ar.zip`;
    if (fs.existsSync(zip)) {
        fs.unlinkSync(zip);
    }
    const output = fs.createWriteStream(zip);
    const archive = archiver("zip", { zlib: { level: 1 } });

    output.on("close", () => {
        console.log(`${archive.pointer()} total bytes`);
        console.log("archiver has been finalized and the output file descriptor has closed.");
    });
    output.on("end", () => {
        console.log("Data has been drained");
    });
    archive.on("warning", (err) => {
        if (err.code === "ENOENT") {
            console.warn(err);
        } else {
            throw err;
        }
    });
    archive.on("error", (err) => {
        throw err;
    });

    archive.pipe(output);
    console.log("Downloading and zipping");
    const success = await Promise.all(images.filter(Boolean).map((Key) => processImage(Key, archive)));
    await archive.finalize();
    const status = success.filter(Boolean);
    if (status.length) {
        console.log("Uploading zip", `${firstId}-${lastId}.zip`);
        await aws.s3.putObject({
            Bucket: "doubtnut-user-uploads/archive",
            Key: `${firstId}-${lastId}.zip`,
            Body: fs.createReadStream(zip),
            StorageClass: "DEEP_ARCHIVE",
        }).promise();
    }
    const s = status.length;
    // TODO delete images
    await deleteImages(status);
    fs.unlinkSync(zip);
    return s;
}

async function start() {
    const data = await getQAImages();
    if (!data) {
        console.log("No new data");
        return;
    }
    const status = await processImages(data.firstId, data.lastId, data.images);
    console.log("status", status);
    if (status) {
        await setLastArchivedQid(data.firstId, data.lastId);
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "* * * * *",
    removeOnComplete: 10,
    removeOnFail: 20,
};
