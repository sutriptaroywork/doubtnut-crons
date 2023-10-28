/* eslint-disable no-await-in-loop */
const { mysql, puppeteer, aws } = require("../../modules");

async function getPendingPdfs() {
    const sql = "select * from last_day_liveclass_pdfs where pdf_created=0";
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function updateRow(id, key) {
    const sql = `update last_day_liveclass_pdfs set pdf_link='${key}', pdf_created=1 where id=${id}`;
    return mysql.writePool.query(sql).then((res) => res[0]);
}

async function start(job) {
    const browser = await puppeteer.getBrowser();
    const rows = await getPendingPdfs();
    for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        if (!row.html) {
            console.log("null html");
            await updateRow(row.id, "");
            continue;
        }
        const ddmmyyyy = row.date.toISOString()
            .substring(0, 10)
            .split("-")
            .reverse()
            .join("");
        const fileName = `AAJKI_CLASSES_PDF_${ddmmyyyy}_${row.assortment_id}.pdf`;
        const Key = `PDF/${row.entity_type}/${row.date.toISOString().substring(0, 10).replace(/-/g, "/")}/${fileName}`;
        const page = await browser.newPage();
        await page.setContent(row.html, { waitUntil: "networkidle2", timeout: 0 });
        const buf = await page.pdf(puppeteer.pdfOptions);
        await page.close();

        await aws.s3.putObject({
            Bucket: "doubtnut-static",
            Body: buf,
            Key,
            ContentType: "application/pdf",
        }).promise();
        console.log("pdf created", Key);
        await updateRow(row.id, Key);
        await job.progress(parseInt(((i + 1) * 100) / rows.length));
    }
    return 1;
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 2 * * *",
    removeOnComplete: 7,
    removeOnFail: 7,
};
