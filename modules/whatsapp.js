const axios = require("axios");
const http = require("http");
const https = require("https");
const { microUrl } = require("./config");

/**
 * @type {axios.AxiosInstance}
 */
const ax = axios.create({
    httpAgent: new http.Agent({ keepAlive: true, maxSockets: 100 }),
    httpsAgent: new https.Agent({ keepAlive: true, maxSockets: 100 }),
    baseURL: microUrl,
    headers: { "Content-Type": "application/json" },
});

async function sendTextMsg(campaign, phone, studentId, text, hsmData, header, footer, replyType, action, campaignEndTime) {
    return ax.put("/api/whatsapp/send-text-msg", {
        phone,
        studentId,
        text,
        preview: false,
        campaign,
        hsmData,
        header,
        footer,
        action,
        replyType,
        campaignEndTime,
        bulk: true,
    }, {
        timeout: 20000,
    });
}

async function sendMediaMsg(campaign, phone, studentId, mediaUrl, mediaType, caption, replyType, action, footer, campaignEndTime) {
    return ax.put("/api/whatsapp/send-media-msg", {
        phone,
        studentId,
        mediaUrl,
        mediaType,
        caption,
        campaign,
        replyType,
        action,
        footer,
        campaignEndTime,
        bulk: true,
    }, {
        timeout: 20000,
    });
}

module.exports = {
    sendTextMsg,
    sendMediaMsg,
};
