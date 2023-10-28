const axios = require("axios");

async function sendMessage(channel, blocks, authKey) {
    return axios.post("https://slack.com/api/chat.postMessage", { channel, blocks },
        {
            headers: {
                authorization: `Bearer ${authKey}`,
            },
        });
}
module.exports = {
    sendMessage,
};
