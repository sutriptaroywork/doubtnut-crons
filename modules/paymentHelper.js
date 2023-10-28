const _ = require("lodash");
const request = require("request");
const config = require("./config");

const bufferObj = Buffer.from(`${config.RAZORPAY_KEY_ID}:${config.RAZORPAY_KEY_SECRET}`, "utf8");
const base64String = bufferObj.toString("base64");

async function fetchPaymentsByOrderId(order_id) {
    try {
        const options = {
            method: "GET",
            url: `https://api.razorpay.com/v1/orders/${order_id}/payments?count=50`,
            headers: {
                Authorization: `Basic ${base64String}`,
            },
        };
        const rzpResponse = await new Promise((resolve, reject) => {
            request(options, (error, response) => {
                if (error) reject(error);
                // if (response) resolve(response.body);
                resolve(response.body);
            });
        });

        return JSON.parse(rzpResponse);
    } catch (e) {
        console.log(e);
    }
}

module.exports = {
    fetchPaymentsByOrderId,
};
