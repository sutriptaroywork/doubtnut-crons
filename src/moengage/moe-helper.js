const moment = require("moment");
const crypto = require("crypto");
const rp = require("request-promise");
const { v4: uuid } = require("uuid");
const { config, getMongoClient } = require("../../modules");

const db = "doubtnut";
const moeCampaigns = "moe_campaigns";
const moeCSV = "moe_csvs";

function delay(ms = 10000) {
    return new Promise((resolve) => {
        setTimeout(() => {
            resolve();
        }, ms);
    });
}

async function generateXiaomiThumb(thumb) {
    try {
        const buf = await rp.get(thumb, { encoding: null });
        const options = {
            url: "https://api.xmpush.global.xiaomi.com/media/upload/image",
            headers: {
                Authorization: `key=${config.moengage.xiaomiKey}`,
            },
            formData: {
                file: {
                    value: buf,
                    options: {
                        filename: thumb.split("/").splice(-1)[0],
                        contentType: null,
                    },
                },
                is_global: "true",
                is_icon: "false",
            },
            json: true,
            timeout: 5000,
        };

        const response = await rp.post(options);
        // console.log(response);
        return response.data.pic_url;
    } catch (e) {
        // console.error(e);
    }
}

async function scheduleNotification(segmentName, liveAt, questionId, thumbnail, message, meta, retry = 0) {
    if ((segmentName.startsWith("CSV") && retry > 6) || (!segmentName.startsWith("CSV") && retry > 1)) {
        return;
    }
    const xiaomiThumb = await generateXiaomiThumb(thumbnail);
    const richContent = thumbnail ? [
        {
            type: "image",
            value: thumbnail,
        },
    ] : null;
    if (richContent && xiaomiThumb) {
        richContent.push({
            type: "mi_image_url",
            value: xiaomiThumb,
        });
    }
    const payload = richContent
        ? {
            ANDROID: {
                message: message.msg,
                title: message.title,
                richContent,
                defaultAction: {
                    type: "deeplinking",
                    value: `doubtnutapp://live_class?id=${questionId}&page=LIVECLASS_NOTIFICATION`,
                },
            },
        } : {
            ANDROID: {
                message: message.msg,
                title: message.title,
                defaultAction: {
                    type: "deeplinking",
                    value: `doubtnutapp://live_class?id=${questionId}&page=LIVECLASS_NOTIFICATION`,
                },
            },
        };

    console.log(`${segmentName}\t${liveAt}\t${questionId}\t${thumbnail}\t${message.title}\t${message.msg}`);
    const { appId } = config.moengage;
    const { apiSecret } = config.moengage;
    const campaignName = meta === "replay" ? `${questionId}-${segmentName}-${liveAt.toISOString().substr(0, 10)}-replay` : `${questionId}-${segmentName}-${liveAt.toISOString().substr(0, 16)}`;
    const signatureBody = `${appId}|${campaignName}|${apiSecret}`.toString("utf8");
    const body = {
        requestId: uuid(),
        appId,
        campaignName,
        signature: crypto.createHash("sha256").update(signatureBody).digest("hex"),
        requestType: "push",
        targetPlatform: [
            "ANDROID",
        ],
        targetAudience: "Custom Segment",
        customSegmentName: segmentName,
        payload,
        campaignDelivery: {
            type: "later",
            date: moment(liveAt).format("MM/DD/YYYY"),
            time: moment(liveAt).format("hh:mm A"),
        },
        advancedSettings: {
            ttl: {
                ANDROID: 5,
            },
            ignoreFC: "false",
            countFC: "true",
            sendAtHighPriority: "true",
            preCacheSegment: "true",
            "pushAmp+": "true",
        },
        conversionGoals: [
            {
                name: "Video_Watch",
                eventName: "video_watched",
                attrs: {
                    filter: "is",
                    name: "QuestionId",
                    value: questionId.toString(),
                    type: "string",
                },
            },
            {
                name: "Video_Page",
                eventName: "VideoPage",
                attrs: {
                    filter: "is",
                    name: "QuestionId",
                    value: questionId.toString(),
                    type: "string",
                },
            },
        ],
    };
    const client = (await getMongoClient()).db(db);
    console.log(body);
    try {
        const response = await rp.post({
            url: "https://pushapi.moengage.com/v2/transaction/sendpush/priority",
            json: true,
            body,
            timeout: 10000,
        });
        console.log(response);
        client.collection(moeCampaigns).insertOne({
            body, retry, response, meta,
        }, { ordered: false });
        if (response.status === "Success") {
            return true;
        }
        if (segmentName.startsWith("CSV")) {
            await delay();
        } else {
            await delay(2000);
        }
        console.log("retrying", retry + 1);
        return scheduleNotification(segmentName, liveAt, questionId, thumbnail, message, meta, retry + 1);
    } catch (err) {
        console.error(err);
        client.collection(moeCampaigns).insertOne({
            body, retry, err, meta,
        }, { ordered: false });
        await delay(2000);
        console.log("retrying", retry + 1);
        return scheduleNotification(segmentName, liveAt, questionId, thumbnail, message, meta, retry + 1);
    }
}

async function createCSVSegment(questionId, segmentName, csvPath) {
    console.log(`${questionId}\t${segmentName}\t${csvPath}`);
    const { appId } = config.moengage;
    const { apiSecret } = config.moengage;
    const segName = `CSV-${questionId}-${segmentName}`;
    const signatureBody = `${appId}|${segName}|${apiSecret}`.toString("utf8");
    const body = {
        segName,
        callback: "https://foo.bar.com",
        csvUrl: csvPath,
        appId,
        attribute: "student_id",
        signature: crypto.createHash("sha256").update(signatureBody).digest("hex"),
        attributeType: "string",
        emails: ["rupal.shree@doubtnut.com"],
    };
    const client = (await getMongoClient()).db(db);
    console.log(body);
    try {
        const response = await rp.post({
            url: "https://api.moengage.com/csv_segment",
            json: true,
            body,
            timeout: 10000,
        });
        console.log(response);
        client.collection(moeCSV).insertOne({
            body, response,
        }, { ordered: false });
        if (response.status === 200) {
            return segName;
        }
    } catch (err) {
        console.error(err);
        client.collection(moeCSV).insertOne({
            body, err,
        }, { ordered: false });
        return segName;
    }
}

module.exports = {
    createCSVSegment,
    scheduleNotification,
};
