const dotenv = require("dotenv");

dotenv.config({ path: "./.env" });

console.log(process.env.NODE_ENV);

module.exports = {
    prod: process.env.NODE_ENV === "CRON",
    staticBucket: "doubtnut-static",
    staticCDN: "https://d10lpgp6xz60nq.cloudfront.net/",
    staticCloudfrontCDN: "https://d10lpgp6xz60nq.cloudfront.net/",
    queueRedis: {
        host: process.env.QUEUE_REDIS,
        password: process.env.QUEUE_REDIS_PASS,
    },
    mysql: {
        host: {
            read: process.env.MYSQL_HOST_READ,
            write: process.env.MYSQL_HOST_WRITE,
        },
        user: process.env.MYSQL_USER,
        password: process.env.MYSQL_PASS,
    },
    mongo: process.env.MONGO_URL,
    moengage: {
        appId: process.env.MOENGAGE_APP_ID,
        apiSecret: process.env.MOENGAGE_API_SECRET,
        xiaomiKey: process.env.MOENGAGE_XIAOMI_KEY,
    },
    pagerdutyKey: process.env.PAGERDUTY_KEY,
    newtonUrl: `${process.env.MICRO_URL}/api/newton/notification/send`,
    redis: {
        backend: {
            hosts: (process.env.REDIS_CLUSTER_HOSTS || "").split(","),
            password: process.env.REDIS_CLUSTER_PASSWORD,
            db: process.env.REDIS_CLUSTER_DATABASE || 0,
        },
        liveclass: {
            hosts: (process.env.REDIS_LIVECLASS_HOST || "").split(","),
            password: process.env.REDIS_LIVECLASS_PASS,
            db: process.env.REDIS_LIVECLASS_DATABASE || 0,
            graphName: process.env.REDIS_LIVECLASS_GRAPH_NAME || "liveclass",
        },
    },
    branch_key: process.env.BRANCH_KEY,
    branch_key_iit: process.env.IIT_BRANCH_KEY,
    branch_key_neet: process.env.NEET_BRANCH_KEY,
    firebase_key: process.env.FIREBASE_KEY,
    fermi: {
        maxInstanceCount: 50,
        instanceType: process.env.FERMI_INSTANCE_TYPE || "c4.xlarge",
        amiId: process.env.FERMI_AMI_ID,
    },
    sendgrid_key: process.env.SENDGRID_KEY,
    elasticsearch: {
        host1: process.env.ELASTIC_HOST_1,
        host2: process.env.ELASTIC_HOST_2,
        host3: process.env.ELASTIC_HOST_3,
        host4: process.env.ELASTIC_HOST_4,
    },
    gupshup: {
        transactional: {
            userid: process.env.GUPSHUP_TRANSACTIONAL_USERID,
            password: process.env.GUPSHUP_TRANSACTIONAL_PASSWORD,
        },
        promotional: {
            userid: process.env.GUPSHUP_PROMOTIONAL_USERID,
            password: process.env.GUPSHUP_PROMOTIONAL_PASSWORD,
        },
    },
    microUrl: process.env.MICRO_URL || "https://gateway.doubtnut.internal/",
    flagr: {
        microUrl: process.env.MICRO_URL,
    },
    redshift: {
        host: process.env.REDSHIFT_HOST,
        user: process.env.REDSHIFT_USER,
        password: process.env.REDSHIFT_PASSWORD,
        database: process.env.REDSHIFT_DATABASE,
        port: process.env.REDSHIFT_PORT,
    },
    paytm: {
        referral: {
            key: process.env.REFERRAL_PAYTM_KEY,
            guid: process.env.REFERRAL_PAYTM_SUBWALLET_GUID,
            mid: process.env.REFERRAL_PAYTM_MID,
        },
        payment: {
            mid: process.env.PAYTM_MID,
            key: process.env.PAYTM_KEY,
        },
    },
    RAZORPAY_KEY_ID: process.env.RAZORPAY_KEY_ID,
    RAZORPAY_KEY_SECRET: process.env.RAZORPAY_KEY_SECRET,
    kafkaHosts: process.env.KAFKA_HOSTS ? process.env.KAFKA_HOSTS.split(",") : [],
    voicemaker_api_key: process.env.VOICEMAKER_API_KEY,
    kaleyra: {
        key: process.env.KALEYRA_VOICE_KEY,
    },
    // This key is to send slack notifications for Payment's Cron to #payments-team and #payments-team-dev channels
    paymentsAutobotSlackAuth: "xoxb-534514142867-3197106116752-AG3DIvAKcx5IsBCKYxhkfCcq",
    waAutobotSlackAuth: "xoxb-534514142867-4006189610580-xjHsoVkIDcSrsxR8CpKOvs15",
    SCHOLARSHIP_SLACK_AUTH: process.env.SCHOLARSHIP_SLACK_AUTH,
    RENEWAL_SLACK_AUTH: process.env.RENEWAL_SLACK_AUTH,
    STICKY_SLACK_AUTH: process.env.STICKY_SLACK_AUTH,
    liveclass: {
        appName: process.env.STREAM_APPNAME,
        vodDomain: process.env.VOD_DOMAIN,
    },
    vdocipherApikey: process.env.VDOCIPHER_API_KEY,
    cdn_video_url: process.env.CDN_VIDEO_URL,
    availableCDNs: [
        {
            cdn: "cloudfront",
            url: "https://d10lpgp6xz60nq.cloudfront.net/",
            weight: 1,
            origin: "https://d10lpgp6xz60nq.cloudfront.net",
            cdn_video_url: "https://d3cvwyf9ksu0h5.cloudfront.net/",
            cdn_video_origin: "https://d3cvwyf9ksu0h5.cloudfront.net",
        },
    ],
    tencent_secret_id: process.env.TENCENT_SECRET_ID,
    tencent_secret_key: process.env.TENCENT_SECRET_KEY,
};
