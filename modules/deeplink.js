const request = require("request");
const _ = require("lodash");
const rp = require("request-promise");
const config = require("./config");

async function generateDeeplinkFromAppDeeplink(channel, campaign, deeplink) {
    // deeplink = 'doubtnutapp://pdf_viewer?pdf_url=${config.staticCDN}pdf_download/JM_2019_ALL.pdf&foo=bar';
    const splitted = deeplink.split("?");
    const featureSplitted = splitted[0].split("//");
    const dataSplitted = splitted[1].split("&");
    const feature = featureSplitted[1];
    const data = {};
    for (let i = 0; i < dataSplitted.length; i++) {
        const s = dataSplitted[i].split("=");
        data[s[0]] = s[1];
    }
    const myJSONObject = {
        branch_key: config.branch_key,
        channel,
        feature,
        campaign,
    };
    if (!_.isEmpty(data)) {
        myJSONObject.data = data;
    }
    const options = {
        url: "https://api.branch.io/v1/url",
        method: "POST",
        json: true,
        body: myJSONObject,
    };
    return rp(options);
}

async function generateDeeplink(channel, campaign, feature, data) {
    // deeplink = 'doubtnutapp://pdf_viewer?pdf_url=${config.staticCDN}pdf_download/JM_2019_ALL.pdf&foo=bar';
    const myJSONObject = {
        branch_key: config.branch_key,
        channel,
        feature,
        campaign,
    };
    if (!_.isEmpty(data)) {
        myJSONObject.data = data;
    }
    const options = {
        url: "https://api.branch.io/v1/url",
        method: "POST",
        json: true,
        body: myJSONObject,
    };
    return rp(options);
}
module.exports = {
    generateDeeplinkFromAppDeeplink,
    generateDeeplink,
};
