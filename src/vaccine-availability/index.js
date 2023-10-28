/* eslint-disable newline-per-chained-call */
/* eslint-disable guard-for-in */
/* eslint-disable no-await-in-loop */
const moment = require("moment");
const rp = require("request-promise");
const fs = require("fs");
const { email } = require("../../modules");

const page = fs.readFileSync("./src/vaccine-availability/page.html", "utf-8");
const blockTemplate = fs.readFileSync("./src/vaccine-availability/block.html", "utf-8");

const districtList = {
    "all@doubtnut.com": [141, 145, 140, 146, 147, 143, 148, 149, 144, 150, 142, 188, 650, 651],
    "ankur.madharia@doubtnut.com": [109],
};

async function fetchAvailability(districtId, today) {
    console.log("District Id, date", districtId, today);
    const availability = [];
    const url = `https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByDistrict?district_id=${districtId}&date=${today}`;
    try {
        const res = await rp(url, {
            json: true,
            headers: {
                "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36",
            },
        });
        for (let i = 0; i < res.centers.length; i++) {
            const center = res.centers[i];
            if (!center.sessions) {
                continue;
            }
            for (let j = 0; j < center.sessions.length; j++) {
                const session = center.sessions[j];
                if (session.min_age_limit === 18 && session.available_capacity_dose1 > 2) {
                    availability.push({
                        location: center.name,
                        date: session.date,
                        state: center.state_name,
                        district: center.district_name,
                        block: center.block_name,
                        address: center.address,
                        pincode: center.pincode,
                        vaccine: session.vaccine,
                        capacity: session.available_capacity,
                    });
                }
            }
        }
    } catch (e) {
        console.error(e);
    }
    return availability;
}

async function start() {
    for (const emailId in districtList) {
        const availability = [];
        const districts = districtList[emailId];
        for (let i = 0; i < districts.length; i++) {
            const districtId = districts[i];
            let x = await fetchAvailability(districtId, moment().add(5, "hour").add(30, "minute").format("DD-MM-YYYY"));
            availability.push(...x);
            x = await fetchAvailability(districtId, moment().add(7, "day").add(5, "hour").add(30, "minute").format("DD-MM-YYYY"));
            availability.push(...x);
        }
        if (!availability.length) {
            return {};
        }
        let blocks = "";
        for (let i = 0; i < availability.length; i++) {
            const x = availability[i];
            let template = blockTemplate;
            for (const key in x) {
                template = template.replace(`##${key}##`, x[key]);
            }
            blocks += template;
            console.log(x);
        }
        const html = page.replace("##BLOCK##", blocks);

        const to = [
            emailId,
        ];

        await email.sendEmail(to, "VACCINE AVAILABILITY", html);
    }
    return {};
}

module.exports.start = start;
module.exports.opts = {
    cron: "*/15 0-2,9-23 * * *",
    removeOnComplete: 5,
    removeOnFail: 5,
    disabled: true,
};
