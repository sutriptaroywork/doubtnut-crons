/* eslint-disable no-await-in-loop */
const request = require("request");
const _ = require("lodash");
const {
    mysql, gupshup, notification, deeplink, sendgridMail, slack,
} = require("../../modules");
const PaytmCheckSum = require("./checksum.js");
const config = require("../../modules/config");

/**
 * Legend student_referral_paytm_disbursement
 * 1- successfully disbursement
 * 2- accepted/pending/failed disbursement
 * 3- retry disbursment with retry phone no
 * 4- marked for disbursement
 * 5- razorpay payout link sent
*/

const bufferObj = Buffer.from(`${config.RAZORPAY_KEY_ID}:${config.RAZORPAY_KEY_SECRET}`, "utf8");
const base64String = bufferObj.toString("base64");

async function disburse(phoneNo, orderId, amount) {
    try {
        const paytmParams = {
            orderId,
            subwalletGuid: config.paytm.referral.guid,
            amount,
            beneficiaryPhoneNo: phoneNo,
            // callbackUrl: 'https://paytm.com/test/',
        };
        const post_data = JSON.stringify(paytmParams);
        const checksum_lib = PaytmCheckSum;
        const checksum = await new Promise((resolve, reject) => {
            checksum_lib.genchecksumbystring(post_data, config.paytm.referral.key, (err, data) => {
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
                // 'Content-Length': post_data.length,
            },
            body: post_data,
        };
        console.log(options);
        const paytmResponse = await new Promise((resolve, reject) => {
            request(options, (error, response) => {
                if (error) throw new Error(error);
                // const res = JSON.parse(response);
                console.log(response.body);
                resolve(response.body);
            });
        });
        // console.log(paytmResponse);
        return JSON.parse(paytmResponse);
    } catch (e) {
        console.log(e);
        return 0;
    }
}

async function createPayoutLink(mobile, amount) {
    try {
        const accountNumber = "4564568991002242";
        const date = new Date();
        date.setDate(date.getDate() + 7);
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
            description: "Razorpay Payout Referral",
            send_sms: true,
            notes: {},
            // expire_by: Math.floor(date / 1000),
        };

        const options = {
            method: "POST",
            url: "https://api.razorpay.com/v1/payout-links",
            headers: {
                Authorization: `Basic ${base64String}`,
                "Content-Type": "application/json",
            },
            body: JSON.stringify(payload),
        };

        console.log(options);
        const rzpPayoutResponse = await new Promise((resolve, reject) => {
            request(options, (error, response) => {
                if (error) reject(error);
                resolve(response.body);
            });
        });

        console.log("rzpPayoutResponse", rzpPayoutResponse);
        return JSON.parse(rzpPayoutResponse);
    } catch (e) {
        console.log(e);
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

        console.log(options);
        const rzpPayoutResponse = await new Promise((resolve, reject) => {
            request(options, (error, response) => {
                if (error) reject(error);
                resolve(response.body);
            });
        });

        console.log("rzpPayoutResponse", rzpPayoutResponse);
        return JSON.parse(rzpPayoutResponse);
    } catch (e) {
        console.log(e);
    }
}

async function disbursementStatus(orderId) {
    try {
        const paytmParams = {
            orderId,
        };
        const post_data = JSON.stringify(paytmParams);
        const checksum_lib = PaytmCheckSum;
        const checksum = await new Promise((resolve, reject) => {
            checksum_lib.genchecksumbystring(post_data, config.paytm.referral.key, (err, data) => {
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
                // 'Content-Length': post_data.length,
            },
            body: post_data,
        };
        console.log(options);
        const paytmResponse = await new Promise((resolve, reject) => {
            request(options, (error, response) => {
                if (error) throw new Error(error);
                // const res = JSON.parse(response);
                console.log(response.body);
                resolve(response.body);
            });
        });
        // console.log(paytmResponse);
        return JSON.parse(paytmResponse);
    } catch (e) {
        console.log(e);
        return 0;
    }
}

async function getNewDisbursements() {
    const sql = "select a.*, b.student_id, b.locale, b.gcm_reg_id from (select * from student_referral_paytm_disbursement where (is_paytm_disbursed is null or is_paytm_disbursed = 3)) as a left join students as b on a.invitor_student_id=b.student_id";
    console.log(sql);
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getPendingDisbursements() {
    const sql = "select a.*, b.student_id, b.locale, b.gcm_reg_id from (select * from student_referral_paytm_disbursement where is_paytm_disbursed in (2,5)) as a left join students as b on a.invitor_student_id=b.student_id";
    console.log(sql);
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getStudentDetails(studentID) {
    const sql = `select * from students where student_id=${studentID}`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getDnPropertyValueByName(method) {
    const sql = "select value from dn_property where bucket = 'payout' and name = ? and is_active =1";
    return mysql.pool.query(sql, [method]).then((res) => res[0]);
}

async function updateRzpDisbursal(obj) {
    const sql = "update student_referral_paytm_disbursement set ? where id = ?";
    return mysql.writePool.query(sql, [obj, obj.id]).then((res) => res[0]);
}

async function updateDisbursements(item, is_paytm_disbursed, response, isRetry) {
    let sql;
    if (isRetry) {
        sql = `update student_referral_paytm_disbursement set is_paytm_disbursed=${is_paytm_disbursed}, paytm_response_retry = '${JSON.stringify(response)}' where id=${item.id}`;
    } else {
        sql = `update student_referral_paytm_disbursement set is_paytm_disbursed=${is_paytm_disbursed}, paytm_response = '${JSON.stringify(response)}' where id=${item.id}`;
    }
    const res = await mysql.writePool.query(sql);
    return res;
}

async function createRzpDisbursal(item) {
    if (item.amount >= 1) {
        const markForPayoutObj = {
            source: "RAZORPAY",
            id: item.id,
            is_paytm_disbursed: 4,
        };
        await updateRzpDisbursal(markForPayoutObj);
        const rzpResponse = await createPayoutLink(item.mobile, item.amount);
        if (rzpResponse && rzpResponse.short_url && rzpResponse.id) {
            const updateObj = {
                id: item.id,
                is_paytm_disbursed: 5,
                razorpay_response: JSON.stringify(rzpResponse),
                razorpay_url: rzpResponse.short_url,
                payout_id: rzpResponse.id,
            };
            await updateRzpDisbursal(updateObj);
        }
        return rzpResponse;
    }
}

async function sendDisbursementFailureCommunicationAndUpdateEntry(item, res) {
    const student1 = [];
    const stu = {};
    const rzpResponse = await createRzpDisbursal(item);
    stu.id = item.student_id;
    stu.gcmId = item.gcm_reg_id;
    student1.push(stu);
    const notificationPayload = {
        event: "external_url",
        title: item.locale == "hi" ? "आपकी Paytm राशि आपको भेजी नहीं जा सकी 😔" : "Aapka Paytm Cash Prize transfer nahi ho paya 😔",
        message: item.locale == "hi" ? "पैसे खाते में पाने के लिए, यहाँ क्लिक करें 👇🏻" : "Paise Account me paane ke liye, yahan click karein 👇🏻",
        firebase_eventtag: "REFERRAL_PAYTM_DECLINE",
        s_n_id: "REFERRAL_PAYTM_DECLINE",
        data: JSON.stringify({
            url: rzpResponse.short_url,
        }),
    };
    try {
        await notification.sendNotification(student1, notificationPayload);
    } catch (e) {
        console.log(e);
    }
    await gupshup.sendSMSMethodGet({
        phone: item.is_paytm_disbursed == 3 ? item.mobile_retry : item.mobile,
        msg: `Hello bacche, Doubtnut se aapka ₹${item.amount} PayTm cashback failure fail ho gaya hai because ${res.statusMessage}.
    Apna cashback UPI ya bank mae receive karne ke liye iss link par click karein - ${rzpResponse.short_url}`,
        locale: "en",
    });
}

async function checkRzpPayoutStatus(item) {
    const rzpResponse = await checkPayoutLinkStatus(item.payout_id);
    if (rzpResponse && rzpResponse.status == "processed") {
        const updateObj = {
            id: item.id,
            is_paytm_disbursed: 1,
            razorpay_response: JSON.stringify(rzpResponse),
        };
        await updateRzpDisbursal(updateObj);
    }
}

async function sendNewDisbursementWinnerCommunication(item) {
    const student1 = [];
    const stu = {};
    stu.id = item.student_id;
    stu.gcmId = item.gcm_reg_id;
    student1.push(stu);
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
        await notification.sendNotification(student1, notificationPayload);
    } catch (e) {
        console.log(e);
    }
    if (item.amount == 150) {
        const inviteeDetails = await getStudentDetails(item.invitee_student_id);
        const linkStudentId = Buffer.from(`${item.invitor_student_id}XX${item.id}`).toString("base64");
        const branchDeeplink = await deeplink.generateDeeplink("SMS", "REFERRAL_PAYTM_WINNER", "external_url", { url: `https://doubtnut.com/paytm-referral-success?student_id=${linkStudentId}&paytm_number=x` });
        await gupshup.sendSMSMethodGet({
            phone: item.is_paytm_disbursed == 3 ? item.mobile_retry : item.mobile,
            msg: `Aapke dost(Mobile - *${(inviteeDetails[0].mobile).substr(inviteeDetails[0].mobile.length - 3)}) ne aapke diye hue link se Doubtnut par course khareed kar aapko jeeta diye hain ₹150 Paytm Cash Prize!\n${branchDeeplink.url}`,
            locale: "en",
        });
    }
}

async function sendPendingDisbursementWinnerCommunication(item) {
    const student = [];
    const stu = {};
    stu.id = item.student_id;
    stu.gcmId = item.gcm_reg_id;
    student.push(stu);
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
        await gupshup.sendSMSMethodGet({ phone: !_.isNull(item.mobile_retry) ? item.mobile_retry : item.mobile, msg: `Aapke dost(Mobile - *${(inviteeDetails[0].mobile).substr(inviteeDetails[0].mobile.length - 3)}) ne aapke diye hue link se Doubtnut par course khareed kar aapko jeeta diye hain ₹150 Paytm Cash Prize!\n${branchDeeplink.url}`, locale: "en" });
    }
}

async function start(job) {
    const fromEmail = "autobot@doubtnut.com";
    const toEmail = "prakher.gaushal@doubtnut.com";
    const ccList = ["dipankar@doubtnut.com", "prashant.gupta@doubtnut.com"];
    try {
        const paytmSwitch = await getDnPropertyValueByName("PAYTM");
        const rzpSwitch = await getDnPropertyValueByName("RAZORPAY");

        paytmSwitch[0].value = parseInt(paytmSwitch[0].value);
        rzpSwitch[0].value = parseInt(rzpSwitch[0].value);

        const pendingDisbursements = await getPendingDisbursements();
        job.progress(20);
        if (pendingDisbursements.length > 0) {
            for (const item of pendingDisbursements) {
                const blockNew = [];
                try {
                    // Check if rzp link has been sent -> check status -> if processed {update table} continue to next entry
                    if (item.is_paytm_disbursed == 5) {
                        await checkRzpPayoutStatus(item);
                        continue;
                    }
                    const statusResponse = await disbursementStatus(item.order_id);
                    console.log(statusResponse);
                    if (statusResponse && statusResponse.status == "SUCCESS") {
                        await updateDisbursements(item, 1, statusResponse, _.isNull(item.mobile_retry) ? 0 : 1);
                        await sendPendingDisbursementWinnerCommunication(item);
                    } else if (statusResponse && statusResponse.status) {
                        await updateDisbursements(item, 2, statusResponse, _.isNull(item.mobile_retry) ? 0 : 1);
                    }
                    if (statusResponse && statusResponse.status && ["FAILURE", "CANCELLED"].includes(statusResponse.status) && rzpSwitch[0].value == 1) {
                        await sendDisbursementFailureCommunicationAndUpdateEntry(item, statusResponse);
                    }
                } catch (e) {
                    console.log(e);
                    await sendgridMail.sendMail(fromEmail, toEmail, `CRON | ALERT!!! Exception in Referral Paytm Disbursement. Existing srpd_id: ${item.id} `, JSON.stringify(e), [], ccList);
                    blockNew.push({
                        type: "section",
                        text: { type: "mrkdwn", text: `CRON | ALERT!!! Exception in Referral Paytm Disbursement. Existing srpd_id: ${item.id} <@U01MJU54A21> <@U0273ABLEPL> <@ULGN432HL>:\n\`\`\`${e.stack}\`\`\`` },
                    });
                    await slack.sendMessage("#payments-team", blockNew, config.paymentsAutobotSlackAuth);
                }
            }
        }
        job.progress(50);
        const newDisbursements = await getNewDisbursements();
        console.log(newDisbursements);
        job.progress(70);
        if (newDisbursements.length > 0) {
            for (const item of newDisbursements) {
                const blockNew = [];
                try {
                    if (item.amount > 0 && paytmSwitch[0].value == 1) {
                        const markForPayoutObj = {
                            source: "PAYTM",
                            id: item.id,
                            is_paytm_disbursed: 4,
                        };
                        await updateRzpDisbursal(markForPayoutObj);
                        const res = await disburse(item.is_paytm_disbursed == 3 ? item.mobile_retry : item.mobile, item.order_id, item.amount);
                        if (res && res.status && res.status == "SUCCESS") {
                            await updateDisbursements(item, 1, res, item.is_paytm_disbursed == 3 ? 1 : 0);
                            await sendNewDisbursementWinnerCommunication(item);
                        } else if (res && res.status) {
                            await updateDisbursements(item, 2, res, item.is_paytm_disbursed == 3 ? 1 : 0);
                        }
                        if (res && res.status && ["FAILURE", "CANCELLED"].includes(res.status) && rzpSwitch[0].value == 1) {
                            await sendDisbursementFailureCommunicationAndUpdateEntry(item, res);
                        }
                    } else if (item.amount > 0 && rzpSwitch[0].value == 1) {
                        await createRzpDisbursal(item);
                    }
                } catch (e) {
                    console.log(e);
                    await sendgridMail.sendMail(fromEmail, toEmail, `CRON | ALERT!!! Exception in Referral Paytm Disbursement. New srpd_id: ${item.id} `, JSON.stringify(e), [], ccList);
                    blockNew.push({
                        type: "section",
                        text: { type: "mrkdwn", text: `CRON | ALERT!!! Exception in Referral Paytm Disbursement. New srpd_id: ${item.id} <@U01MJU54A21> <@U0273ABLEPL> <@ULGN432HL>:\n\`\`\`${e.stack}\`\`\`` },
                    });
                    await slack.sendMessage("#payments-team", blockNew, config.paymentsAutobotSlackAuth);
                }
            }
        }
        job.progress(100);
        return { data: "success" };
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
