/* eslint-disable no-await-in-loop */
const https = require("https");
const AWS = require("aws-sdk");
const { execSync } = require("child_process");
const path = require("path");
const axios = require("axios");
const fs = require("fs");
const config = require("../../modules/config");
const { mysql } = require("../../modules");

async function getNullCheckoutAudio() {
    const sql = "select * from audio_repo where url is null and is_active=1 order by id desc";
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function setCheckoutAudioURL(url, id) {
    const sql = "update audio_repo set url = ?, is_active = 1 where id = ?";
    return mysql.writePool.query(sql, [url, id]).then((res) => res[0]);
}

async function download(url, fpath) {
    return new Promise((resolve) => {
        https.get(url, (res) => {
            const filePath = fs.createWriteStream(fpath);
            res.pipe(filePath);
            filePath.on("finish", () => {
                filePath.close();
                console.log(`Downloaded File ${fpath}`);
                resolve();
            });
        });
    });
}

async function deleteFile(filePath) {
    return new Promise((resolve, reject) => {
        fs.unlink(filePath, (err) => {
            if (err) {
                console.log(err);
                reject();
            } else {
                console.log(`Deleted file: ${filePath}`);
                resolve();
            }
        });
    });
}

const s3 = new AWS.S3();
function uploadToS3(uploadParams, filePath) {
    return new Promise(async (resolve, reject) => {
        s3.upload(uploadParams, async (err, data) => {
            if (err) {
                reject(err);
                throw err;
            }
            await deleteFile(filePath);
            resolve();
        });
    });
}

async function uploadToAws(fileName) {
    const body = fs.readFileSync(path.resolve(__dirname, `../../${fileName}`));
    const uploadParams = {
        Bucket: "doubtnut-static", Key: `audio_repository/${fileName}`, Body: body, ContentType: "audio/mp3",
    };
    await uploadToS3(uploadParams, path.resolve(__dirname, `../../${fileName}`));
    console.log(`Uploaded ${fileName}`);
}

async function textToSpeech(text) {
    try {
        const payload = {
            Engine: "neural",
            VoiceId: "ai2-hi-IN-Anamika",
            LanguageCode: "hi-IN",
            Text: `${text}`,
            OutputFormat: "mp3",
            SampleRate: "16000",
            Effect: "default",
            MasterSpeed: "0",
            MasterVolume: "0",
            MasterPitch: "0",
        };
        const options = {
            method: "POST",
            url: "https://developer.voicemaker.in/voice/api",
            headers: {
                Authorization: `Bearer ${config.voicemaker_api_key}`,
                "Content-Type": "application/json",
            },
            data: JSON.stringify(payload),
            json: true,
        };
        console.log(options);
        const voicemakerResponse = (await axios(options)).data;
        return JSON.parse(JSON.stringify(voicemakerResponse));
    } catch (e) {
        console.log(e);
    }
}

async function createCheckoutAudio() {
    const audioToCreateList = await getNullCheckoutAudio();
    for (let i = 0; i < audioToCreateList.length; i++) {
        try {
            const speechResponse = await textToSpeech(audioToCreateList[i].text);
            if (speechResponse.success) {
                const audioRepoId = audioToCreateList[i].id;
                const assortmentId = audioToCreateList[i].entity_id;
                const downloadURL = speechResponse.path;
                const inFile = `assortment_id_${assortmentId}_in.mp3`;
                const outFile = `assortment_id_${assortmentId}_out.mp3`;
                const s3URL = `https://d10lpgp6xz60nq.cloudfront.net/audio_repository/${outFile}`;

                await download(downloadURL, `./${inFile}`);
                console.log(`Iteration ${i} Step 1 (Downloaded File ${inFile})`);

                await execSync(`ffmpeg -i ${inFile} -ab 16k ${outFile} -y`);
                console.log(`Iteration ${i} Step 2 (ffmpeg Compressed ${outFile})`);

                await uploadToAws(outFile);
                await deleteFile(inFile);
                await setCheckoutAudioURL(s3URL, audioRepoId);
                console.log(`Iteration ${i} Step 3 (Uploaded and Updated ${assortmentId})`);
            } else {
                console.log("Voicemaker API Error");
                console.log(speechResponse);
            }
        } catch (e) {
            console.log("e2", e);
            throw new Error(e);
        }
    }
}

async function start(job) {
    try {
        await createCheckoutAudio();
        return { err: null, data: null };
    } catch (e) {
        console.log(e);
        return { e };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 0 1 6 *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
