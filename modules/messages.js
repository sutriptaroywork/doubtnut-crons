const axios = require("axios");
const request = require("request");
const config = require("./config");

async function sendSms(params, transactional = true) {
    try {
        console.log("Sending SMS", params.phone, params.msg, params.locale);
        const options = {
            method: "post",
            url: "http://enterprise.smsgupshup.com/GatewayAPI/rest",
            data: {
                method: "sendMessage",
                send_to: params.phone,
                msg: params.msg,
                msg_type: params.locale === "hi" ? "Unicode_Text" : "TEXT",
                encoding: "Unicode_Text",
                userid: transactional ? config.gupshup.transactional.userid : config.gupshup.promotional.userid,
                auth_scheme: "PLAIN",
                password: transactional ? config.gupshup.transactional.password : config.gupshup.promotional.password,
                format: "JSON",
                v: "1.1",
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

async function sendSMSMethodGet(data, transactional = true) {
    return new Promise(((resolve, reject) => {
        const options = {
            method: "GET",
            url: "https://enterprise.smsgupshup.com/GatewayAPI/rest",
            qs:
                {
                    method: "SendMessage",
                    send_to: data.phone,
                    msg: data.msg,
                    msg_type: data.locale === "hi" ? "Unicode_Text" : "TEXT",
                    userid: transactional ? config.gupshup.transactional.userid : config.gupshup.promotional.userid,
                    auth_scheme: "plain",
                    data_encoding: "Unicode_text",
                    password: transactional ? config.gupshup.transactional.password : config.gupshup.promotional.password,
                    v: "1.1",
                    format: "JSON",
                },
        };
        console.log(options);
        request(options, (error, response) => {
            if (error) throw new Error(error);
            console.log(response.body);
            if (error) reject(error);
            resolve(response.body);
        });
    }));
}

module.exports = {
    sendSms,
    sendSMSMethodGet,
};
