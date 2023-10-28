const { SESV2 } = require("aws-sdk");

const ses = new SESV2({
    region: "ap-south-1",
    signatureVersion: "v4",
});

async function sendEmail(to, subject, html) {
    try {
        const res = await ses.sendEmail({
            FromEmailAddress: "autobot@doubtnut.com",
            Destination: {
                ToAddresses: to,
            },
            Content: {
                Simple: {
                    Subject: {
                        Data: subject,
                    },
                    Body: {
                        Html: {
                            Data: html,
                        },
                    },
                },
            },
        }).promise();
        console.log(res);
    } catch (e) {
        console.error(e);
    }
}

module.exports = {
    sendEmail,
};
