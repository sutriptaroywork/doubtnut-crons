const axios = require("axios");
const helper = require("sendgrid").mail;

const email_list = ["akshat@doubtnut.com", "akanksha@doubtnut.com"]; // for testing
const config = require("../../modules/config");

const sendGridMailSecretKey = config.sendgrid_key;
const sendgrid = require("sendgrid")(sendGridMailSecretKey);

async function getClusterDetails(cluster_endpoint) {
    const elastic_cluster_details = [];
    return new Promise((resolve, reject) => {
        axios.get(`${cluster_endpoint}_cat/indices`).then((res) => {
            const esResponse = res.data;
            const dataRows = esResponse.split("\n");
            for (let i = 0; i < dataRows.length - 1; i++) {
                const index_details = dataRows[i].split(/\s+/);
                const index_name = index_details[2];
                const index_doc_count = index_details[6];
                elastic_cluster_details.push({ index_name, index_doc_count });
            }
            resolve(elastic_cluster_details);
        }).catch((err) => {
            console.log(err);
        });
    });
}

function obj2HtmlTable(arr) {
    try {
        const cols = Object.keys(arr[0]);
        const headerRow = cols.map((col) => `<th>${col}</th>`).join("");
        const rows = arr.map((row) => {
            const tds = cols.map((col) => `<td>${row[col]}</td>`).join("");
            return `<tr>${tds}</tr>`;
        }).join("");

        return `
            <table>
                <thead>
                    <tr>${headerRow}</tr>
                <thead>
                <tbody>
                    ${rows}
                <tbody>
            <table>
        `;
    } catch (e) {
        console.log(e);
    }
}

async function sendMail(content_html) {
    try {
        for (let i = 0; i < email_list.length; i++) {
            const subject = "Number of Documents by index";
            const from_email = new helper.Email("autobot@doubtnut.com");
            const to_email = new helper.Email(email_list[i]);
            const content = new helper.Content("text/html", content_html);
            const mail = new helper.Mail(from_email, subject, to_email, content);
            const sg = await sendgrid.emptyRequest({
                method: "POST",
                path: "/v3/mail/send",
                body: mail.toJSON(),
            });
            const response = await sendgrid.API(sg);
        }
        return true;
    } catch (e) {
        console.log(e);
    }
}

async function start(job) {
    try {
        let html = "";
        const elasticClusters = Object.values(config.elasticsearch);
        for (let i = 0; i < elasticClusters.length; i++) {
            const cluster_details = await getClusterDetails(elasticClusters[i]);
            html += `<br>
                <h3>Cluster${i} DETAILS :- </h3>    
            <br>`;
            const cluster_details_html = obj2HtmlTable(cluster_details);
            html += cluster_details_html;
        }
        await sendMail(html);
        await job.progress(100);
        return { data: "SUCCESS" };
    } catch (e) {
        console.log(e);
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 3 * * *",
};
