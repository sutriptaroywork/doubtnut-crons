const axios = require("axios");
const config = require("./config");

async function getFlagrResp(flgrData, timeout = 500) {
    try {
        const headers = { "Content-Type": "application/json" };
        if (flgrData.xAuthToken) {
            headers["x-auth-token"] = flgrData.xAuthToken;
        }
        const { data } = await axios({
            method: "POST",
            url: `${config.microUrl}api/app-config/flagr`,
            timeout,
            headers,
            data: flgrData.body,
        });
        return data;
    } catch (e) {
        console.error(e);
    }
}

module.exports = {
    getFlagrResp,
};
