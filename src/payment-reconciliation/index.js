/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const Razorpay = require("razorpay");
const fs = require("fs");
const moment = require("moment");
const crypto = require("crypto");
const request = require("request");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;
const { default: axios } = require("axios");
const config = require("../../modules/config");
const { getMongoClient } = require("../../modules");
const {
    mysql, sendgridMail, paymentHelper, slack,
} = require("../../modules");

const db = "doubtnut";
const paymentCollection = "payment_webhook";

function gen_salt(length, cb) {
    crypto.randomBytes((length * 3.0) / 4.0, (err, buf) => {
        let salt;
        if (!err) {
            salt = buf.toString("base64");
        }
        // salt=Math.floor(Math.random()*8999)+1000;
        cb(err, salt);
    });
}

function encrypt(data, custom_key) {
    const { iv } = this;
    const key = custom_key;
    let algo = "256";
    // eslint-disable-next-line default-case
    switch (key.length) {
        case 16:
            algo = "128";
            break;
        case 24:
            algo = "192";
            break;
        case 32:
            algo = "256";
            break;
    }
    const cipher = crypto.createCipheriv(`AES-${algo}-CBC`, key, iv);
    // var cipher = crypto.createCipher('aes256',key);
    let encrypted = cipher.update(data, "binary", "base64");
    encrypted += cipher.final("base64");
    return encrypted;
}

function genchecksumbystring(params, key, cb) {
    gen_salt(4, (err, salt) => {
        const sha256 = crypto.createHash("sha256").update(`${params}|${salt}`).digest("hex");
        const check_sum = sha256 + salt;
        const encrypted = encrypt(check_sum, key);

        let CHECKSUMHASH = encodeURIComponent(encrypted);
        CHECKSUMHASH = encrypted;
        cb(undefined, CHECKSUMHASH);
    });
}

async function getPaymentsByDate(from, to, minCreatedAt) {
    const sql = "select pi.* from classzoo1.payment_info as pi where pi.updated_at > ? and pi.updated_at < ? and pi.created_at > ? and (pi.source = 'RAZORPAY' or pi.source = 'razorpay' or pi.source = 'PAYTM') and pi.coupon_code <> 'internal' and pi.student_id NOT IN (select student_id from classzoo1.internal_subscription) order by id desc";
    return mysql.pool.query(sql, [from, to, minCreatedAt]).then((res) => res[0]);
}

async function getPaymentByPartnerTxnID(txnID) {
    const sql = "select pi.*, pr.amount as refund_amount, pr.status as refund_status from payment_info pi left join payment_refund pr on pi.id = pr.payment_info_id where pi.partner_txn_id = ? order by id desc limit 1";
    return mysql.pool.query(sql, [txnID]).then((res) => res[0]);
}

async function getPaymentByPartnerOrderId(orderID) {
    const sql = "select * from payment_info where partner_order_id = ? order by id desc limit 1";
    return mysql.pool.query(sql, [orderID]).then((res) => res[0]);
}

async function getVirtualAccountDetailsByVirtualAccountId(vaID) {
    const sql = "select * from payment_info_smart_collect where virtual_account_id = ? order by id desc limit 1";
    return mysql.pool.query(sql, [vaID]).then((res) => res[0]);
}

async function paytmTransactionStatus(orderId) {
    const paytmParams = {};

    paytmParams.body = {

        mid: config.PAYTM.MID,

        orderId,
    };

    const checksum = await new Promise((resolve, reject) => {
        genchecksumbystring(JSON.stringify(paytmParams.body), config.PAYTM.KEY, (err, data) => {
            resolve(data);
        });
    });

    paytmParams.head = {
        signature: checksum,
    };
    const post_data = JSON.stringify(paytmParams);

    const options = {

        url: `${config.PAYTM.BASE_URL}/merchant-status/api/v1/getPaymentStatus`,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            "Content-Length": post_data.length,
        },
        json: true,
        body: paytmParams,
    };

    console.log(options);

    const paytmResponse = await new Promise((resolve, reject) => {
        request(options, (error, response) => {
            if (error) throw new Error(JSON.stringify(error));

            resolve(response.body);
        });
    });

    return paytmResponse.body.resultInfo.resultStatus;
}

async function razorPayCheckWithStatus(razorPayResponse, paymentInfoStatus) {
    const response = [false, "NO ENTRY IN RZP"];
    if (!_.isEmpty(razorPayResponse) && razorPayResponse.count > 0) {
        for (let i = 0; i < razorPayResponse.count; i++) {
            if (response[1] == "captured");
            else if (response[1] == "failed" && razorPayResponse.items[i].status != "captured");
            else {
                response[1] = razorPayResponse.items[i].status;
            }
            if ((razorPayResponse.items[i].status == "captured") && (paymentInfoStatus == "SUCCESS")) {
                response[0] = true;
                return response;
            }
            if ((razorPayResponse.items[i].status == "failed") && (paymentInfoStatus == "FAILURE" || paymentInfoStatus == "INITIATED")) {
                response[0] = true;
                return response;
            }
            if ((razorPayResponse.items[i].status == "created") && (paymentInfoStatus == "INITIATED")) {
                response[0] = true;
                return response;
            }
        }
    } else if (paymentInfoStatus == "INITIATED" || paymentInfoStatus == "RECONCILE") {
        response[0] = true;
        response[1] = "NA";
        return response;
    }
    return response;
}

async function paytmCheckWithStatus(payment) {
    // console.log(payment);
    const paytmTxnStatus = await paytmTransactionStatus(payment.order_id);
    // console.log(paytmTxnStatus);
    if (paytmTxnStatus == "TXN_SUCCESS" && payment.status == "SUCCESS") {
        return true;
    } if (paytmTxnStatus == "TXN_FAILURE" && payment.status == "FAILURE") {
        return true;
    } if (paytmTxnStatus == "PENDING" && payment.status == "PENDING") {
        return true;
    } if (payment.status == "INITIATED") {
        return true;
    }
    return false;
}

async function fetchRazorPayPayments(rzp, from, to) {
    let rzpPayments = [];
    let rzpResponseLength = 0;
    let rzpResponseAll = [];
    const maxIteration = 20;
    try {
        do {
            const promiseRzp = [];
            // eslint-disable-next-line no-await-in-loop
            // console.log("rzpResponse",rzpResponse.length);
            for (let i = 0; i < maxIteration; i++) {
                const skip = 100 * i + rzpResponseLength;
                promiseRzp.push(rzp.payments.all({
                    from,
                    to,
                    count: 100,
                    skip,
                }));
            }

            rzpResponseAll = await Promise.all(promiseRzp);

            console.log("rzpResponseAll", rzpResponseAll);
            for (let i = 0; i < rzpResponseAll.length; i++) {
                const rzpResponse = rzpResponseAll[i];
                rzpPayments = rzpPayments.concat(rzpResponse.items);
                rzpResponseLength += rzpResponse.count;
            }
            console.log(rzpResponseLength);
        } while (rzpResponseAll[maxIteration - 1].count > 0);
        return rzpPayments;
    } catch (e) {
        console.log("e4", e);
        throw new Error(JSON.stringify(e));
    }
}

async function resolveRazorpaySmartCollectDiscrepancies(webhookData) {
    delete webhookData._id;
    const options = {
        url: "https://api.doubtnut.com/v1/payment/rzp-hook",
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        data: webhookData,
    };
    const { data } = await axios(options);
    console.log("data", data);
    const piEntry = await getPaymentByPartnerTxnID(webhookData.payload.payment.entity.id);
    if (!_.isEmpty(piEntry)) {
        return true;
    }
    return false;
}

async function reconcileRzpPaymentsWithPITable(rzpPayments) {
    try {
        const missingPayments = [];

        const iterationLevel = 100;
        const rzpClone = JSON.parse(JSON.stringify(rzpPayments));
        let batch = rzpClone.splice(0, iterationLevel);

        const paymentInfoResponses = [];
        while (batch.length) {
            const promises = [];
            for (let i = 0; i < batch.length; i++) {
                promises.push(getPaymentByPartnerTxnID(batch[i].id));
            }
            batch = rzpClone.splice(0, iterationLevel);
            // const paymentInfoResponses = await Promise.all(promises);
            const ss = await Promise.all(promises);

            for (let i = 0; i < ss.length; i++) {
                paymentInfoResponses.push(ss[i][0]);
            }
        }

        for (let i = 0; i < rzpPayments.length; i++) {
            if (!_.isEmpty(paymentInfoResponses[i]) && !_.isEmpty(rzpPayments[i])) {
                console.log("entry exist in pi");
                if (rzpPayments[i].status == "captured" && (paymentInfoResponses[i].status == "SUCCESS" || paymentInfoResponses[i].status == "RECONCILE"));
                else if (rzpPayments[i].status == "failed" && paymentInfoResponses[i].status == "FAILURE");
                else if (rzpPayments[i].status == "captured" && (paymentInfoResponses[i].status == "FAILURE" || paymentInfoResponses[i].status == "INITIATED")) {
                    const finalDatum = {};
                    const paymentMethod = rzpPayments[i].bank || rzpPayments[i].wallet;
                    finalDatum.amount = (rzpPayments[i].amount / 100);
                    finalDatum.pgType = "RAZORPAY";
                    finalDatum.revenue = (rzpPayments[i].amount / 100);
                    finalDatum.discount = "";
                    finalDatum.coupon_code = "";
                    finalDatum.amount_paid = (rzpPayments[i].amount / 100);
                    finalDatum.payment_id = rzpPayments[i].id;
                    finalDatum.payment_info_id = null;
                    finalDatum.rzp_status = rzpPayments[i].status == "captured" ? "SUCCESS" : rzpPayments[i].status;
                    finalDatum.payment_method = paymentMethod || "razorpay";
                    finalDatum.refund = (rzpPayments[i].amount_refunded / 100);
                    finalDatum.payment_info_status = `${rzpPayments[i].error_code}: Wrong Entry In PI`;
                    finalDatum.student_id = paymentInfoResponses[i].student_id;
                    finalDatum.transaction_time = moment(paymentInfoResponses[i].created_at).format("YYYY-MM-DD HH:mm:ss");
                    missingPayments.push(finalDatum);
                } else if ((rzpPayments[i].status == "created") && (paymentInfoResponses[i].status == "INITIATED"));
                else if (((rzpPayments[i].status == "refunded") && (paymentInfoResponses[i].refund_status == "SUCCESS") && ((rzpPayments[i].amount_refunded / 100) == paymentInfoResponses[i].refund_amount)) || ((rzpPayments[i].status == "refunded") && paymentInfoResponses[i].payment_for != "wallet"));
                else {
                    const finalDatum = {};
                    finalDatum.amount = (rzpPayments[i].amount / 100);
                    finalDatum.pgType = "RAZORPAY";
                    finalDatum.revenue = (rzpPayments[i].amount / 100);
                    finalDatum.discount = paymentInfoResponses[i].discount;
                    finalDatum.coupon_code = paymentInfoResponses[i].coupon_code;
                    finalDatum.amount_paid = (rzpPayments[i].amount / 100);
                    finalDatum.payment_id = rzpPayments[i].id;
                    finalDatum.payment_info_id = paymentInfoResponses[i].id;
                    finalDatum.rzp_status = rzpPayments[i].status == "captured" ? "SUCCESS" : rzpPayments[i].status;
                    finalDatum.payment_method = paymentInfoResponses[i].mode;
                    finalDatum.refund = (rzpPayments[i].amount_refunded / 100);
                    finalDatum.payment_info_status = `${rzpPayments[i].error_code}: Wrong Entry In PI`;
                    finalDatum.student_id = paymentInfoResponses[i].student_id;
                    finalDatum.transaction_time = moment(paymentInfoResponses[i].created_at).format("YYYY-MM-DD HH:mm:ss");
                    missingPayments.push(finalDatum);
                }
            } else if (!_.isEmpty(rzpPayments[i]) && rzpPayments[i].status == "captured") {
                const finalDatum = {};
                console.log("entry not in pi", rzpPayments[i]);
                const paymentMethod = rzpPayments[i].bank || rzpPayments[i].wallet;
                finalDatum.amount = (rzpPayments[i].amount / 100);
                finalDatum.pgType = "RAZORPAY";
                finalDatum.revenue = (rzpPayments[i].amount / 100);
                finalDatum.discount = "";
                finalDatum.coupon_code = "";
                finalDatum.amount_paid = (rzpPayments[i].amount / 100);
                finalDatum.payment_id = rzpPayments[i].id;
                finalDatum.payment_info_id = null;
                finalDatum.rzp_status = rzpPayments[i].status == "captured" ? "SUCCESS" : rzpPayments[i].status;
                finalDatum.payment_method = paymentMethod || "razorpay";
                finalDatum.refund = (rzpPayments[i].amount_refunded / 100);
                finalDatum.payment_info_status = `${rzpPayments[i].error_code}: Missing Entry In PI`;
                finalDatum.transaction_time = moment.unix(rzpPayments[i].created_at).format("YYYY-MM-DD HH:mm:ss");
                finalDatum.student_id = "";
                // eslint-disable-next-line no-await-in-loop
                const client = (await getMongoClient()).db(db);
                // eslint-disable-next-line no-await-in-loop
                const paymentWebhookData = JSON.parse(JSON.stringify(await client.collection(paymentCollection).find({
                    "payload.payment.entity.id": `${rzpPayments[i].id}`,
                }).toArray()));
                console.log("webhook", paymentWebhookData);
                let missingPaymentResolved = false;
                if (!_.isEmpty(paymentWebhookData)) {
                    const virtualAccountEventIndex = _.findIndex(paymentWebhookData, (o) => o.event == "virtual_account.credited");
                    const paymentLinkEventIndex = _.findIndex(paymentWebhookData, (o) => o.event == "payment_link.paid");
                    const paymentCapturedEventIndex = _.findIndex(paymentWebhookData, (o) => o.event == "payment.captured");
                    console.log(virtualAccountEventIndex, paymentLinkEventIndex, paymentCapturedEventIndex);
                    if (paymentLinkEventIndex > -1) {
                        const paymentInfo = await getPaymentByPartnerOrderId(paymentWebhookData[paymentLinkEventIndex].payload.payment_link.entity.id);
                        if (!_.isEmpty(paymentInfo)) {
                            finalDatum.student_id = paymentInfo[0].student_id;
                            finalDatum.payment_info_id = paymentInfo[0].id;
                        } else {
                            finalDatum.student_id = `mobile: ${rzpPayments[i].contact}`;
                        }
                        missingPaymentResolved = await resolveRazorpaySmartCollectDiscrepancies(paymentWebhookData[paymentLinkEventIndex]);
                    } else if (virtualAccountEventIndex > -1) {
                        const virtualAccountDetails = await getVirtualAccountDetailsByVirtualAccountId(paymentWebhookData[virtualAccountEventIndex].payload.virtual_account.entity.id);
                        if (!_.isEmpty(virtualAccountDetails)) {
                            finalDatum.student_id = virtualAccountDetails[0].student_id;
                        } else {
                            finalDatum.student_id = `mobile: ${rzpPayments[i].contact}`;
                        }
                        missingPaymentResolved = await resolveRazorpaySmartCollectDiscrepancies(paymentWebhookData[virtualAccountEventIndex]);
                    } else if (paymentCapturedEventIndex > -1) {
                        const paymentInfo = await getPaymentByPartnerOrderId(paymentWebhookData[paymentCapturedEventIndex].payload.payment.entity.order_id);
                        if (!_.isEmpty(paymentInfo)) {
                            finalDatum.student_id = paymentInfo[0].student_id;
                            finalDatum.payment_info_id = paymentInfo[0].id;
                        } else {
                            finalDatum.student_id = `mobile: ${rzpPayments[i].contact}`;
                        }
                        missingPaymentResolved = await resolveRazorpaySmartCollectDiscrepancies(paymentWebhookData[paymentCapturedEventIndex]);
                    }
                }
                if (!missingPaymentResolved) {
                    missingPayments.push(finalDatum);
                }
            }
        }
        return missingPayments;
    } catch (e) {
        console.log("e3", e);
        throw new Error(JSON.stringify(e));
    }
}

async function reconcilePIEntriesWithRzp(paymentInfoPayments) {
    const flaggedPayments = [];
    const paytmPromises = [];
    try {
        let i = 0;
        console.log("paymentInfoPaymentslength", paymentInfoPayments.length);
        _.remove(paymentInfoPayments, (n) => (n.source.toLowerCase() != "razorpay" || n.partner_order_id == null || !(n.partner_order_id.split("_")[0].includes("order"))));
        console.log("paymentInfoPaymentslength", paymentInfoPayments.length);
        while (i < paymentInfoPayments.length) {
            const iterationLevel = i + 25;
            const promises = [];
            for (; i < paymentInfoPayments.length && i <= iterationLevel; i++) {
                // console.log(paymentInfoPayments[i].partner_order_id);
                promises.push(paymentHelper.fetchPaymentsByOrderId(paymentInfoPayments[i].partner_order_id));
            }
            const response = await Promise.all(promises);
            console.log(response);
            const rzpStatusPromises = [];
            for (let j = 0; j < response.length; j++) {
                rzpStatusPromises.push(razorPayCheckWithStatus(response[j], paymentInfoPayments[iterationLevel - 25 + j].status));
            }
            const statusResponses = await Promise.all(rzpStatusPromises);
            console.log(statusResponses);
            for (let j = 0; j < response.length; j++) {
                if (!statusResponses[j][0]) {
                    const finalDatum = {};
                    // eslint-disable-next-line no-nested-ternary
                    const payment_id = _.isEmpty(response[j]) ? null : response[j].count == 0 ? null : response[j].items[0].id;
                    finalDatum.amount = paymentInfoPayments[iterationLevel - 25 + j].amount;
                    finalDatum.pgType = paymentInfoPayments[iterationLevel - 25 + j].source;
                    finalDatum.revenue = paymentInfoPayments[iterationLevel - 25 + j].amount;
                    finalDatum.discount = paymentInfoPayments[iterationLevel - 25 + j].discount;
                    finalDatum.coupon_code = paymentInfoPayments[iterationLevel - 25 + j].coupon_code;
                    finalDatum.amount_paid = paymentInfoPayments[iterationLevel - 25 + j].amount;
                    finalDatum.payment_id = paymentInfoPayments[iterationLevel - 25 + j].partner_txn_id || payment_id;
                    finalDatum.payment_info_id = paymentInfoPayments[iterationLevel - 25 + j].id;
                    finalDatum.payment_info_status = paymentInfoPayments[iterationLevel - 25 + j].status;
                    finalDatum.rzp_status = statusResponses[j][1];
                    finalDatum.payment_method = paymentInfoPayments[iterationLevel - 25 + j].mode;
                    finalDatum.refund = "";
                    finalDatum.student_id = paymentInfoPayments[iterationLevel - 25 + j].student_id;
                    finalDatum.transaction_time = moment(paymentInfoPayments[iterationLevel - 25 + j].created_at).format("YYYY-MM-DD HH:mm:ss");
                    flaggedPayments.push(finalDatum);
                }
            }
        }
        console.log("pi rzp done");
        // paytm Reconciliation
        // for (let j = 0; j < remainingPaments.length; j++) {
        //     if (remainingPaments[i].source == "PAYTM") {
        //         paytmPromises.push(paytmCheckWithStatus(paymentInfoPayments[i]));
        //     }
        // }
        // const paytmReconcileStatuses = await Promise.all(paytmPromises);
        // for (let j = 0; j < paytmReconcileStatuses.length; j++) {
        //     if (!paytmReconcileStatuses[i]) {
        //         const finalDatum = {};
        //         finalDatum.amount = remainingPaments[j].amount;
        //         finalDatum.pgType = remainingPaments[j].source;
        //         finalDatum.revenue = remainingPaments[j].amount;
        //         finalDatum.discount = remainingPaments[j].discount;
        //         finalDatum.coupon_code = remainingPaments[j].coupon_code;
        //         finalDatum.amount_paid = remainingPaments[j].amount;
        //         finalDatum.payment_id = remainingPaments[j].partner_txn_id;
        //         finalDatum.payment_info_id = remainingPaments[j].id;
        //         finalDatum.payment_status = remainingPaments[j].status;
        //         finalDatum.payment_method = remainingPaments[j].mode;
        //         finalDatum.refund = "";
        //         finalDatum.error_code = "";
        //         // console.log(finalDatum);
        //         flaggedPayments.push(finalDatum);
        //     }
        // }

        return flaggedPayments;
    } catch (e) {
        console.log("e2", e);
        throw new Error(JSON.stringify(e));
    }
}

async function generateCSVFile(finalpayments, reportName) {
    try {
        const headerKeys = Object.keys(finalpayments[0]);
        const header = [];
        for (let i = 0; i < headerKeys.length; i++) {
            header.push({
                id: headerKeys[i],
                title: headerKeys[i],
            });
        }
        console.log(header);
        const csvWriter = createCsvWriter({
            path: `${reportName}.csv`,
            header,
        });
        await csvWriter.writeRecords(finalpayments);
    } catch (e) {
        console.log("csv error", e);
        throw new Error(JSON.stringify(e));
    }
}

async function deleteFiles(rzpFile, piFile) {
    try {
        fs.unlinkSync(rzpFile);
        fs.unlinkSync(piFile);
    } catch (e) {
        console.log(e);
    }
}

async function start(job) {
    const fromEmail = "autobot@doubtnut.com";
    const toEmail = "prashant.gupta@doubtnut.com";
    const ccList = ["dipankar@doubtnut.com", "aditya@doubtnut.com", "gauravm@doubtnut.com", "harshit.gupta@doubtnut.com", "aakash.dwivedi@doubtnut.com"];
    const blockNew = [];
    try {
        const rzp = new Razorpay({
            key_id: config.RAZORPAY_KEY_ID,
            key_secret: config.RAZORPAY_KEY_SECRET,
        });
        const paymentEntriesFromDate = moment().add(5, "h").add(30, "minutes").subtract(1, "d")
            .startOf("day");
        const paymentEntriesToDate = moment().add(5, "h").add(30, "minutes").subtract(1, "d")
            .endOf("day");
        const minCreatedAt = moment().add(5, "h").add(30, "minutes").subtract(16, "d")
            .startOf("day");
        // console.log(paymentEntriesFromDate);
        // console.log(paymentEntriesToDate);
        // return {err: null, data: null};
        // Reconciling Rzp Payment Entries In payment_info table
        const rzpPayments = await fetchRazorPayPayments(rzp, paymentEntriesFromDate.unix(), paymentEntriesToDate.unix());
        // console.log(rzpPayments);
        const rzpReconciliation = await reconcileRzpPaymentsWithPITable(rzpPayments);
        console.log("rzp reconcilde done");
        // Reconciling payment_info Entries with Rzp and Paytm
        const paymentsByDate = await getPaymentsByDate(paymentEntriesFromDate.format("YYYY-MM-DD HH:mm:ss"), paymentEntriesToDate.format("YYYY-MM-DD HH:mm:ss"), minCreatedAt.format("YYYY-MM-DD HH:mm:ss"));
        console.log("pi entries fetched");
        const paymentInfoReconciliation = await reconcilePIEntriesWithRzp(paymentsByDate);

        try {
            const date = moment().add(5, "h").add(30, "minutes").subtract(1, "d")
                .format("YYYY-MM-DD");
            const prevDate = moment().add(5, "h").add(30, "minutes").subtract(2, "d")
                .format("YYYY-MM-DD");
            // deleteFiles(`RazorPayReconciliation_${prevDate}.csv`, `PaymentInfoReconciliation_${prevDate}.csv`);
            const filesList = [];
            if (rzpReconciliation.length) {
                await generateCSVFile(rzpReconciliation, `RazorPayReconciliation_${date}`);
                filesList.push(`RazorPayReconciliation_${date}.csv`);
                blockNew.push({
                    type: "section",
                    text: { type: "mrkdwn", text: `CRON | Payment Reconciliation cron ran successfully:\n*Razorpay Reconciliation Daily Discrepancies:*\n\`\`\`${JSON.stringify(rzpReconciliation.map((obj) => obj))}\`\`\`` },
                });
            }
            if (paymentInfoReconciliation.length) {
                await generateCSVFile(paymentInfoReconciliation, `PaymentInfoReconciliation_${date}`);
                filesList.push(`PaymentInfoReconciliation_${date}.csv`);
                blockNew.push({
                    type: "section",
                    text: { type: "mrkdwn", text: `CRON | Payment Reconciliation cron ran successfully:\n*Payment Info Reconciliation Daily Discrepancies:*\n\`\`\`${JSON.stringify(paymentInfoReconciliation.map((obj) => obj))}\`\`\`` },
                });
            }
            await slack.sendMessage("#payments-team-dev", blockNew, config.paymentsAutobotSlackAuth);
            await sendgridMail.sendMail(fromEmail, toEmail, "CRON | Payment Reconciliation cron ran successfully", "Daily Report - Payment Reconciliation", filesList, ccList);
            // else {
            //     sendgridMail.sendMail(fromEmail, toEmail, "CRON | Payment Reconciliation cron ran successfully", "No discrepancy found", [], ccList);
            // }
        } catch (e) {
            console.log(e);
            await sendgridMail.sendMail(fromEmail, toEmail, "CRON | ALERT!!! Exception in Payment Reconciliation ", JSON.stringify(e), [], ccList);
            blockNew.push({
                type: "section",
                text: { type: "mrkdwn", text: `CRON | ALERT!!! Exception in Payment Reconciliation <@U01MJU54A21> <@U0273ABLEPL> <@ULGN432HL>:\n\`\`\`${e.stack}\`\`\`` },
            });
            await slack.sendMessage("#payments-team", blockNew, config.paymentsAutobotSlackAuth);
        }
        return { err: null, data: null };
    } catch (e) {
        console.log("e1", e);
        await sendgridMail.sendMail(fromEmail, toEmail, "CRON | ALERT!!! Exception in Payment Reconciliation ", JSON.stringify(e), [], ccList);
        blockNew.push({
            type: "section",
            text: { type: "mrkdwn", text: `CRON | ALERT!!! Exception in Payment Reconciliation <@U01MJU54A21> <@U0273ABLEPL> <@ULGN432HL>:\n\`\`\`${e.stack}\`\`\`` },
        });
        await slack.sendMessage("#payments-team", blockNew, config.paymentsAutobotSlackAuth);
        return { e };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "07 00 * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
