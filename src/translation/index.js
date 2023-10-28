/* eslint-disable no-await-in-loop */
const AWS = require("aws-sdk");
const path = require("path");
const fs = require("fs");
const _ = require("lodash");
const moment = require("moment");
const { mysql } = require("../../modules");

const locales = ["en", "hi", "bn", "gu", "kn", "ml", "mr", "ne", "pa", "ta", "te", "ur"];
const localeArr = [];

for (let i = 0; i < locales.length; i++) {
    localeArr.push({
        locale: locales[i],
        namespace: (locales[i] === "hi" || locales[i] === "en") ? ["translation", "asset", "exception", "other"] : ["translation", "asset"],
    });
}

async function getTranslationByLocale(locale) {
    const sql = `select t8.key, ${locale}, updated_at, namespace from t8 where ${locale} is not null and is_active = 1 order by updated_at desc`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function deleteFile(filePath) {
    return new Promise((resolve, reject) => {
        fs.unlink(filePath, (err) => {
            if (err) {
                console.log(err);
                reject(err);
            } else {
                console.log(`Deleted file: ${filePath}`);
                resolve();
            }
        });
    });
}

const s3 = new AWS.S3();
function uploadToS3(uploadParams) {
    return new Promise((resolve, reject) => {
        s3.upload(uploadParams, async (err, data) => {
            if (err) {
                reject(err);
                throw err;
            }
            resolve(data);
        });
    });
}

async function uploadToAws(filePath, locale, namespace) {
    const body = fs.readFileSync(filePath);
    const uploadParams = {
        Bucket: "dn-locale",
        Key: `production/${locale}/${namespace}.json`,
        Body: body,
    };
    await uploadToS3(uploadParams);
    console.log(`Uploaded ${locale}/${namespace}.json`);
}

/**
 * 1. Assume all data to be populated
 * 2. make json file from t8
 * 3. upload json to s3
 */

async function start(job) {
    try {
        for (let i = 0; i < localeArr.length; i++) {
            try {
                const translationList = await getTranslationByLocale(localeArr[i].locale);
                let lastUpdatedDiff = 60;
                if (!_.isEmpty(translationList)) {
                    lastUpdatedDiff = moment().add(5, "h").add(30, "minutes").diff(moment(translationList[0].updated_at), "minutes");
                }
                console.log(`lastUpdatedDiff_${localeArr[i].locale}`, lastUpdatedDiff);
                if (lastUpdatedDiff < 60) {
                    const translations = _.groupBy(translationList, "namespace");
                    for (const namespace of Object.keys(translations)) {
                        const resObj = {};
                        for (let j = translations[namespace].length - 1; j >= 0; j--) {
                            console.log(translations[namespace][j].key, translations[namespace][j][localeArr[i].locale]);
                            resObj[translations[namespace][j].key] = translations[namespace][j][localeArr[i].locale];
                        }
                        if (!fs.existsSync(`locales/${localeArr[i].locale}`)) {
                            fs.mkdirSync(`locales/${localeArr[i].locale}`, { recursive: true });
                        }
                        fs.writeFileSync(`locales/${localeArr[i].locale}/${namespace}.json`, JSON.stringify(resObj, null, "\t"));
                        const filePath = path.resolve(__dirname, `../../locales/${localeArr[i].locale}/${namespace}.json`);
                        await uploadToAws(filePath, localeArr[i].locale, namespace);
                        await deleteFile(filePath);
                    }
                }
            } catch (e) {
                console.log(e);
                throw new Error(e);
            }
        }
        return { err: null, data: null };
    } catch (e) {
        console.log(e);
        return { e };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "11 * * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
