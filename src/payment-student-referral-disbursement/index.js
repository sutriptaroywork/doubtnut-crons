/* eslint-disable no-await-in-loop */
/* eslint-disable eqeqeq */
const axios = require("axios");
const moment = require("moment");
const {
    config, mysql, gupshup, notification, deeplink, sendgridMail, slack,
} = require("../../modules");
const PaytmCheckSum = require("../../modules/paytm/checksum");

/**
 * Legend student_referral_disbursement
 * CREATED - default
 * MARKED- marked for disbursements (To be handled manually if exists)
 * INITIATED - disbursement initiated
 * FAILURE- disbursement failed
 * CANCELLED- disbursement cancelled
 * SUCCESS- disbursement successful
*/

const bufferObj = Buffer.from(`${config.RAZORPAY_KEY_ID}:${config.RAZORPAY_KEY_SECRET}`, "utf8");
const base64String = bufferObj.toString("base64");

async function paytmDisburse(phoneNo, orderId, amount) {
    try {
        const paytmParams = {
            orderId,
            subwalletGuid: config.paytm.referral.guid,
            amount,
            beneficiaryPhoneNo: phoneNo,
            maxQueueDays: 0,
            // callbackUrl: 'https://paytm.com/test/',
        };
        const postData = JSON.stringify(paytmParams);
        const checksumLib = PaytmCheckSum;
        const checksum = await new Promise((resolve, reject) => {
            checksumLib.genchecksumbystring(postData, config.paytm.referral.key, (err, data) => {
                if (err) reject(err);
                resolve(data);
            });
        });
        const options = {
            url: "https://dashboard.paytm.com/bpay/api/v1/disburse/order/wallet/gratification",
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "x-mid": config.paytm.referral.mid,
                "x-checksum": checksum,
                // 'Content-Length': postData.length,
            },
            data: postData,
        };
        const paytmResponse = await (await axios(options)).data;
        return JSON.parse(JSON.stringify(paytmResponse));
    } catch (e) {
        console.log(e);
        return {};
    }
}

async function paytmDisbursementStatus(orderId) {
    try {
        const paytmParams = {
            orderId,
        };
        const postData = JSON.stringify(paytmParams);
        const checksumLib = PaytmCheckSum;
        const checksum = await new Promise((resolve, reject) => {
            checksumLib.genchecksumbystring(postData, config.paytm.referral.key, (err, data) => {
                if (err) reject(err);
                resolve(data);
            });
        });
        const options = {
            url: "https://dashboard.paytm.com/bpay/api/v1/disburse/order/query",
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "x-mid": config.paytm.referral.mid,
                "x-checksum": checksum,
                // 'Content-Length': postData.length,
            },
            data: postData,
        };
        const paytmResponse = await (await axios(options)).data;
        return JSON.parse(JSON.stringify(paytmResponse));
    } catch (e) {
        console.log(e);
        return {};
    }
}

async function cancelPaytmDisbursement(orderId) {
    try {
        const paytmParams = {
            paytmOrderIds: [orderId],
        };
        const postData = JSON.stringify(paytmParams);
        const checksumLib = PaytmCheckSum;
        const checksum = await new Promise((resolve, reject) => {
            checksumLib.genchecksumbystring(postData, config.paytm.referral.key, (err, data) => {
                if (err) reject(err);
                resolve(data);
            });
        });
        const options = {
            url: "https://dashboard.paytm.com/bpay/api/v1/disburse/order/cancel",
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "x-mid": config.paytm.referral.mid,
                "x-checksum": checksum,
                // 'Content-Length': postData.length,
            },
            data: postData,
        };
        const paytmResponse = await (await axios(options)).data;
        return JSON.parse(JSON.stringify(paytmResponse));
    } catch (e) {
        console.log(e);
        return {};
    }
}

async function createPayoutLink(mobile, amount) {
    try {
        // Razorpay Virtual Account
        const accountNumber = "4564568991002242";
        const date = new Date();
        date.setDate(date.getDate() + 30);
        const payload = {
            account_number: accountNumber,
            contact: {
                name: "Referral Student",
                contact: mobile,
                type: "customer",
            },
            amount: amount * 100,
            currency: "INR",
            purpose: "cashback",
            description: "Doubtnut CEO Referral",
            send_sms: true,
            notes: {},
            expire_by: Math.floor(date / 1000),
        };

        const options = {
            method: "POST",
            url: "https://api.razorpay.com/v1/payout-links",
            headers: {
                Authorization: `Basic ${base64String}`,
                "Content-Type": "application/json",
            },
            data: JSON.stringify(payload),
        };

        const rzpPayoutResponse = (await axios(options)).data;
        console.log("rzpPayoutResponse", rzpPayoutResponse);
        return JSON.parse(JSON.stringify(rzpPayoutResponse));
    } catch (e) {
        console.log(e);
        return {};
    }
}

async function checkPayoutLinkStatus(payoutId) {
    try {
        const options = {
            method: "GET",
            url: `https://api.razorpay.com/v1/payout-links/${payoutId}`,
            headers: {
                Authorization: `Basic ${base64String}`,
            },
        };

        const rzpPayoutResponse = (await axios(options)).data;
        console.log("rzpPayoutResponse", rzpPayoutResponse);
        return JSON.parse(JSON.stringify(rzpPayoutResponse));
    } catch (e) {
        console.log(e);
        return {};
    }
}

async function getDnPropertyValueByName(source) {
    const sql = "select value from dn_property where bucket = 'payout' and name = ? and is_active = 1";
    return mysql.pool.query(sql, [source]).then((res) => res[0]);
}

async function getDisbursementsByStatus(status) {
    const sql = "select a.*, b.student_id, b.locale, b.gcm_reg_id from (select * from student_referral_disbursement where status = ?) as a left join students as b on a.invitor_student_id=b.student_id";
    return mysql.pool.query(sql, [status]).then((res) => res[0]);
}

async function getPendingDisbursements(time) {
    const sql = "select a.*, b.student_id, b.locale, b.gcm_reg_id from (select * from student_referral_disbursement where status = 'INITIATED' and created_at <= ?) as a left join students as b on a.invitor_student_id=b.student_id ";
    return mysql.pool.query(sql, [time]).then((res) => res[0]);
}

async function getStudentDetails(studentID) {
    const sql = "select * from students where student_id = ?";
    return mysql.pool.query(sql, [studentID]).then((res) => res[0]);
}

async function updateDisbursements(obj) {
    const sql = "update student_referral_disbursement set ? where id = ?";
    return mysql.writePool.query(sql, [obj, obj.id]).then((res) => res[0]);
}

async function sendDisbursementWinnerCommunication(item) {
    const student = [{
        id: item.student_id,
        gcmId: item.gcm_reg_id,
    }];
    const notificationPayload = {
        event: "external_url",
        title: item.locale == "hi" ? "मुबारक हो! आप जीते हैं Paytm कैश!💰" : "Mubarak ho! Aap jeete hain Paytm Cash Prize!💰",
        message: item.locale == "hi" ? `₹${item.amount} आपके Paytm वॉलेट में जमा कर दिए गए हैं ` : `₹${item.amount} aapke Paytm Wallet mein transfer kar diye gaye hain 😃`,
        firebase_eventtag: "REFERRAL_PAYTM_WINNER",
        s_n_id: "REFERRAL_PAYTM_WINNER",
        data: JSON.stringify({
            url: `https://doubtnut.com/referral-payment-form?student_id=${Buffer.from(`${item.invitor_student_id}XX${item.id}`).toString("base64")}&paytm_number=x`,
        }),
    };
    try {
        await notification.sendNotification(student, notificationPayload);
    } catch (e) {
        console.log(e);
    }
    if (item.amount == 150) {
        const inviteeDetails = await getStudentDetails(item.invitee_student_id);
        const linkStudentId = Buffer.from(`${item.invitor_student_id}XX${item.id}`).toString("base64");
        const branchDeeplink = await deeplink.generateDeeplink("SMS", "REFERRAL_PAYTM_WINNER", "external_url", { url: `https://doubtnut.com/referral-payment-form?student_id=${linkStudentId}&paytm_number=x` });
        try {
            await gupshup.sendSMSMethodGet({
                phone: item.mobile,
                msg: `Aapke dost(Mobile - *${(inviteeDetails[0].mobile).substr(inviteeDetails[0].mobile.length - 3)}) ne aapke diye hue link se Doubtnut par course khareed kar aapko jeeta diye hain ₹150 Paytm Cash Prize!\n${branchDeeplink.url}`,
                locale: "en",
            });
        } catch (e) {
            console.log(e);
        }
    }
}

async function markForPayout(source, id) {
    await updateDisbursements({
        source,
        id,
        status: "MARKED",
    });
}

async function createRzpDisbursal(item, isNew) {
    if (item.amount >= 1) {
        await markForPayout("RAZORPAY", item.id);
        const rzpResponse = await createPayoutLink(item.mobile, item.amount);
        if (rzpResponse && rzpResponse.short_url && rzpResponse.id) {
            const updateObj = {
                id: item.id,
                status: "INITIATED",
                payout_url: rzpResponse.short_url,
                payout_id: rzpResponse.id,
            };
            if (isNew) {
                updateObj.partner1_txn_id = rzpResponse.id;
                updateObj.partner1_txn_response = JSON.stringify(rzpResponse);
            } else {
                updateObj.partner2_txn_id = rzpResponse.id;
                updateObj.partner2_txn_response = JSON.stringify(rzpResponse);
            }
            await updateDisbursements(updateObj);
        }
        return rzpResponse;
    }
}

async function checkRzpPayoutStatusAndUpdateEntry(item) {
    const rzpResponse = await checkPayoutLinkStatus(item.payout_id);
    if (rzpResponse && rzpResponse.status == "processed") {
        const updateObj = {
            id: item.id,
            status: "SUCCESS",
        };
        if (item.partner2_txn_id == item.payout_id) {
            updateObj.partner2_txn_response = JSON.stringify(rzpResponse);
        } else if (item.partner1_txn_id == item.payout_id) {
            updateObj.partner1_txn_response = JSON.stringify(rzpResponse);
        }
        await updateDisbursements(updateObj);
    }
}

async function sendDisbursementFailureCommunicationAndUpdateEntry(item, res) {
    const rzpResponse = await createRzpDisbursal(item, 0);

    const student = [{
        id: item.student_id,
        gcmId: item.gcm_reg_id,
    }];
    const notificationPayload = {
        event: "external_url",
        title: item.locale == "hi" ? "आपकी Paytm राशि आपको भेजी नहीं जा सकी 😔" : "Aapka Paytm Cash Prize transfer nahi ho paya 😔",
        message: item.locale == "hi" ? "पैसे UPI या बैंक में पाने के लिए, यहाँ क्लिक करें 👇🏻" : "Paise UPI ya Bank me paane ke liye, yahan click karein 👇🏻",
        firebase_eventtag: "REFERRAL_PAYTM_DECLINE",
        s_n_id: "REFERRAL_PAYTM_DECLINE",
        data: JSON.stringify({
            url: rzpResponse.short_url,
        }),
    };
    try {
        await Promise.all([
            notification.sendNotification(student, notificationPayload),
            gupshup.sendSMSMethodGet({
                phone: item.mobile,
                msg: `Hello bacche, Doubtnut se aapka ₹${item.amount} PayTm cashback fail ho gaya hai because ${res.statusMessage}.\nApna cashback UPI ya bank mae receive karne ke liye iss link par click karein - ${rzpResponse.short_url}`,
                locale: "en",
            }),
        ]);
    } catch (e) {
        console.log(e);
    }
}

async function updatePaytmDisbursalStatusEntry(item, response, rzpSwitch) {
    if (response && response.status) {
        if (["SUCCESS"].includes(response.status)) {
            await Promise.all([
                updateDisbursements({
                    id: item.id,
                    status: "SUCCESS",
                    partner1_txn_response: JSON.stringify(response),
                    partner1_txn_id: response.paytmOrderId,
                }),
                sendDisbursementWinnerCommunication(item),
            ]);
        } else if (["FAILURE", "CANCELLED"].includes(response.status)) {
            await updateDisbursements({
                id: item.id,
                status: response.status,
                partner1_txn_response: JSON.stringify(response),
            });
            if (rzpSwitch == 1) {
                await sendDisbursementFailureCommunicationAndUpdateEntry(item, response);
            }
        } else {
            await updateDisbursements({
                id: item.id,
                status: "INITIATED",
                partner1_txn_response: JSON.stringify(response),
            });
        }
    }
}

async function cancelPendingDisbursements(item, rzpSwitch) {
    const response = await cancelPaytmDisbursement(item.order_id);
    if (response && response.status == "ACCEPTED") {
        const statusResponse = await paytmDisbursementStatus(item.order_id);
        await updatePaytmDisbursalStatusEntry(item, statusResponse, rzpSwitch);
    }
}

async function sendErrorReports(blockNew, id, e) {
    const fromEmail = "autobot@doubtnut.com";
    const toEmail = "prakher.gaushal@doubtnut.com";
    const ccList = ["dipankar@doubtnut.com", "prashant.gupta@doubtnut.com"];
    blockNew.push({
        type: "section",
        text: { type: "mrkdwn", text: `Exception in Student Referral Disbursement. id: ${id} <@U01MJU54A21> <@U0273ABLEPL> <@ULGN432HL>:\n\`\`\`${e.stack}\`\`\`` },
    });
    await Promise.all([
        sendgridMail.sendMail(fromEmail, toEmail, `Exception in Student Referral Disbursement. id: ${id}`, e.stack, [], ccList),
        slack.sendMessage("#payments-team", blockNew, config.paymentsAutobotSlackAuth),
    ]);
}

async function start(job) {
    try {
        const time = moment().add(5, "hours").add(30, "minutes").subtract(5, "days")
            .format("YYYY-MM-DD HH:mm:ss");
        const [paytmSwitch, rzpSwitch] = await Promise.all([
            getDnPropertyValueByName("PAYTM"),
            getDnPropertyValueByName("RAZORPAY"),
        ]);
        paytmSwitch[0].value = parseInt(paytmSwitch[0].value);
        rzpSwitch[0].value = parseInt(rzpSwitch[0].value);

        const initiatedDisbursements = await getDisbursementsByStatus("INITIATED");
        if (initiatedDisbursements.length > 0) {
            for (const item of initiatedDisbursements) {
                const blockNew = [];
                try {
                    if (item.source.toUpperCase() == "RAZORPAY") {
                        // Check status of payout link -> if processed {update table} continue to next entry
                        await checkRzpPayoutStatusAndUpdateEntry(item);
                        continue;
                    } else if (item.source.toUpperCase() == "PAYTM") {
                        const statusResponse = await paytmDisbursementStatus(item.order_id);
                        await updatePaytmDisbursalStatusEntry(item, statusResponse, rzpSwitch[0].value);
                    }
                } catch (e) {
                    console.log(e);
                    await sendErrorReports(blockNew, item.id, e);
                }
            }
            const pendingDisbursements = await getPendingDisbursements(time);
            for (const item of pendingDisbursements) {
                const blockNew = [];
                try {
                    if (item.source.toUpperCase() == "PAYTM") {
                        await cancelPendingDisbursements(item, rzpSwitch[0].value);
                    }
                } catch (e) {
                    console.log(e);
                    await sendErrorReports(blockNew, item.id, e);
                }
            }
        }

        job.progress(50);

        const newDisbursements = await getDisbursementsByStatus("CREATED");
        if (newDisbursements.length > 0) {
            for (const item of newDisbursements) {
                const blockNew = [];
                try {
                    if (item.amount > 0 && paytmSwitch[0].value == 1) {
                        await markForPayout("PAYTM", item.id);
                        const res = await paytmDisburse(item.mobile, item.order_id, item.amount);
                        await updatePaytmDisbursalStatusEntry(item, res, rzpSwitch[0].value);
                    } else if (item.amount > 0 && rzpSwitch[0].value == 1) {
                        await createRzpDisbursal(item, 1);
                    }
                } catch (e) {
                    console.log(e);
                    await sendErrorReports(blockNew, item.id, e);
                }
            }
        }

        job.progress(100);

        return { err: null, data: "success" };
    } catch (e) {
        console.log(e);
        return { e };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "30 4-12/2 * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
