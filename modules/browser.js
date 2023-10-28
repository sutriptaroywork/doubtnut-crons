const puppeteer = require("puppeteer");

let browser;

async function getBrowser() {
    if (!browser) {
        console.log("launching browser");
        browser = await puppeteer.launch({
            headless: true,
            executablePath: process.env.CHROME_BIN || null,
            args: ["--no-sandbox", "--headless", "--disable-gpu", "--disable-dev-shm-usage"],
        });
        console.log("browser launched");
    }
    console.log("returning pre launched browser");
    return browser;
}

module.exports = {
    getBrowser,
    pdfOptions: {
        format: "A5",
        margin: {
            top: "0cm",
            left: "1cm",
            right: "1cm",
            bottom: "0cm",
        },
    },
};
