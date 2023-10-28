const config = require("./config");
const _ = require("lodash");
const sendgrid = require("sendgrid")(config.sendgrid_key);
const helper = require("sendgrid").mail;
const fs = require("fs");

async function sendMail(fromEmail, toEmail, subject, body, filesList = null, ccList = null) {
    const from_email = new helper.Email(fromEmail);

    const personalization = new helper.Personalization();
    personalization.addTo(new helper.Email(toEmail));
    // Add Cc mails if passed in the function
    if (!_.isEmpty(ccList)) {
        for (let i = 0; i < ccList.length; i++) {
            personalization.addCc(new helper.Email(ccList[i]));
        }
    }

    const content = new helper.Content("text/plain", body.toString());
    const mail = new helper.Mail();
    mail.setFrom(from_email);
    mail.setSubject(subject);
    mail.addContent(content);
    mail.addPersonalization(personalization);

    // Add CSV Attachment if passed
    if (!_.isEmpty(filesList)) {
        for (let i = 0; i < filesList.length; i++) {
            const attachment = new helper.Attachment();
            const file = fs.readFileSync(filesList[i]);
            const base64File = new Buffer.from(file).toString("base64");
            attachment.setContent(base64File);
            attachment.setType("text/csv");
            attachment.setFilename(filesList[i]);
            attachment.setDisposition("attachment");
            mail.addAttachment(attachment);
        }
    }

    const sg = sendgrid.emptyRequest({
        method: "POST",
        path: "/v3/mail/send",
        body: mail.toJSON(),
    });
    await new Promise((resolve, reject) => {
        sendgrid.API(sg, (error, response) => {
            console.log(response.statusCode);
            console.log("sendgrid_response", response.body);
            resolve(response.body);
            if (error) {
                reject(error);
            }
        //   console.log(response.headers);
        // process.exit(0);
        });
    });
}

module.exports = {
    sendMail,
};
