const { S3, EC2, SQS } = require("aws-sdk");

const s3 = new S3({
    region: "ap-south-1",
    signatureVersion: "v4",
});

const ec2 = new EC2({
    apiVersion: "2016-11-15",
    region: "ap-south-1",
});

const sqs = new SQS({
    region: "ap-south-1",
});

module.exports = {
    s3,
    ec2,
    sqs,
};
