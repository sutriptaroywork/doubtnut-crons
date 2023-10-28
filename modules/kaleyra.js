const axios = require("axios");
const config = require("./config");

async function OBDcall(params, audio) {
    try {
        const options = {
            method: "POST",
            url: `https://api-voice.kaleyra.com/v1/?method=voice.json&api_key=${config.kaleyra.key}&format=json`,
            data: {
                play: audio,
                campaign: "course-purchase",
                call: [{
                    to: params,
                }],
            },
        };
        const { data } = await axios(options);
        console.log(data);
        return data;
    } catch (e) {
        console.error(e);
        return false;
    }
}

module.exports = {
    OBDcall,
};
