/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const moment = require("moment");
const { whatsapp, gupshup } = require("../../modules");
const { getMongoClient, config, mysql } = require("../../modules");

const CAMPAIGN = "QA_PROMO_CRON";
let fails = 0; let success = 0;
// const msg = "👋Hello Students\nKya aapne {{date}} ko hone wale 7️⃣<strong>PM Quiz Contest </strong>🕖 ke liye register kiya hai?.\n\nAgar nahi!!\nToh abhi <strong>register</strong>✅ karein aur <strong>Dhero Prizes</strong>🏆 jeetne ka mauka payein.\n\n<strong>Daily Prizes</strong>\n1st -2️⃣5️⃣0️⃣0️⃣\n2nd-1️⃣5️⃣0️⃣0️⃣\n3rd -1️⃣0️⃣0️⃣0️⃣\n\nIss hafte ka Mega Prize- <strong>mobile phone</strong>📱\n\nContest ki jaankari👇\n{{web_link}}".replace("{{web_link}}", "https://app.doubtnut.com/seven-pm-quiz");
const msgs = [
    "Is physics ke numerical🔢 ko <strong>solve karne ke liye kaunsa formula instemaal hoga</strong> ❓🤔\n\nToh <strong>kabhi bhi kahin bhi</strong> aapke dimag mein🧠 ye khyal aaye bas mujhe uss question ki photo bhej dijiye mein aapko uska sahi jawab✅ dunga😃",
    "Kar liya Apna <strong>home work📝 complete</strong> ya usmein hai kuch doubts❓😥\n\nAgar haan❗ Toh chalo milkar karein aapke <strong>doubts solve</strong> 😃",
    "Kya aapke <strong>doubts rah jaate hain class mein unsolved</strong> ❓😣\n\nToh bas photo bhejo aur ho jayenge aapke saare Doubts solve😀",
    "teacher👩‍🏫👨‍🏫 nahi batati har <strong>Question ka solution baar baar</strong> ❗❗❗ 😪\n\nQuestion ki photo bhejo📲 mein bataunga har <strong>question ka solution har baar</strong> 😊",
    "Classes mein <strong>doubt puchne mein lagta hai darr</strong> ❓😶😥\n\nTo ab hogi darr ke aage jeet 🤩kyuki mere pass hai aapke har <strong>Doubt ka jawaab</strong>",
    "Itni saari Books 📚itne saare Questionsno aur <strong>itne saare Doubts</strong> ❓❗❗❗\nKaha milega jawaab kon karega Doubts solve❓☹\n\n<strong>Milega har jawaab, hoga har Doubt solve sirf yahan</strong> .😀",
    "Mujhse toh ye <strong>Question nahi hone wala</strong> .❗❗❗ 😫\n\nArre aise kaise nahi hoga, Photo bhejiye📲 <strong>Doubtnut ke best teacher👩‍🏫👨‍🏫s jab samjhaege toh har question hoga</strong> 😀❗",
    "Na School teacher👩‍🏫👨‍🏫, Na tuition teacher👩‍🏫👨‍🏫, Na Dost <strong>koi nahi karta mere Doubts solve</strong> .☹\n\nChinta na kar student <strong>Doubtnut Whatsapp</strong> hai na😀 . Ye karega tumhara <strong>har doubt solve</strong>",
    "Yr itna saara syllabus ho gaya hai 📖 <strong>mere purane Doubts kaise honge solve</strong> .😟❓\n\nDoubtnut Whatsapp par <strong>hoga naye, purane, sabhi classes ke aur sabhi subjects ke doubt solve</strong> 😀❗",
    "DOUBTS❗❗ DOUBTS❗❗❗ DOUBTS❗❗❗\n<strong>Kon karega itne Doubts solve</strong> ❓😟\n\n<strong>Physics</strong> ka, <strong>Maths</strong> ka, <strong>Biology</strong> ka, <strong>Chemistry</strong> ka sabka doubt solve karega re tera Doubtnut ❗❗❗😎",
    "<strong>Doubts apne aap thodi solve hote hain</strong> ❗❗\nUnhe solve karna padta hai.📝\n\nToh chalo photo bhejo doubts ki aur video solution dekho📲",
    "<strong>Kya aapko chahiye homework📖 mein help</strong> ❓📖\n\nApne doubts ki photo bhejo, video solution dekho📲 aur apna homework complete karo📝👍",
    "Doubts ko solve karna muskil hi nahi na mumkin hai...\n\nAchaa❗❗❗ <strong>Doubtnut whatsapp par doubt bhej kar dekho yahan har doubt solve hota hai</strong> 😎",
    "<strong>Bachcha Saare Doubts solve karlo</strong> ✍\n<strong>Exams mein marks toh jhak marke aayenge</strong> 😎",
    "Doubtnut par milta hai har doubt ka solution sirf 10 seconds mein❗❗ 😀⏰\n\n<strong>Aap convince ho gaye ya mein aur bolu❓</strong> 😎",
    "Bade Bade deshon mein chhote chhote <strong>doubts nahi hone chahiye</strong> ❗❗😎🚫\n\nToh ab jaldi se apne doubts ke photo bhejo aur video solution paao📲",
    "Aaj mere pass <strong>maths ke doubts hain, physics ke doubts hain, chemistry ke doubts hain, biology ke doubts hain</strong>.\nTumhara pass kya hai❓\n\n<h6>Mere pass unn sab ka solution hai</h6> 😎📝",
    "<h6>Doubts par Doubts, Doubts par Doubts, Doubts par Doubts aate rehte hain par unka solution nahi milta❗❗❗</h6> 😟\n\nAb milega har <strong>Doubt ka video solution sirf 10 seconds mein</strong> 😀⏰",
    "{{user_name}} I hate Doubts ❓❌\n\nInhe jaldi se <strong>Doubtnut Whatsapp par solve karlo</strong> 📝",
    "Padhne se darr nahi lagta sahab, <strong>Doubts se lagta hai</strong> ❗❗❗😟\n\nToh karlo apne saare Doubts whatsapp par solve📱",
    "<h6>Tension lene ka nahi sirf dene ka</h6> ,😎\n\nAur <strong>Doubts rakhne ka nahi, Doubtnut whatsapp par puchh lene ka</strong> 📲",
    "Tum log doubts ke solution dhundh rahe ho 🧐 aur <strong>doubts ke solution yahan whatsapp par</strong> tumhara intezar kar rahe hain😎",
    "Koi Doubt chhota nahi hota aur har doubt ka ek <strong>video solution hota hai jo Doubtnut whatsapp par milta hai</strong> 📲",
    "<h6>Jaa {{user_name}} jaa, Jee le apni Zindagi</h6> 😊\nPuchle apne saare doubts Doubtnut Whatsapp par📲",
    "<h6>Haar kar jeetne wale ko Baazigar kehte hain</h6> 😎\n\nHar <strong>Doubt ka video Solution</strong> dene wale ko Doubtnut whatsapp kehte hain📲",
    "<h6>Itnaa sannata kyun hai bhai❓</h6> 🤔\nKisi ko koi doubts nahi hai kya❓🙄",
    "Kya aapke doubts solve karne ka kisi ke pass time nahi hai❓🙁\n\nToh humse puchho😀 <strong>Doubtnut Whatsapp par milega har doubt ka solution 10 seconds mein</strong> ⏰",
    "<h6>Doubt master Doubtnut naam hai mera</h6> ❗❗\nPhoto lekar <strong>Doubt ka video solution deta hu main</strong>",
    "<h6>{{user_name}} Inn Doubts ke saamne mat harna</h6> ❗❗\n\n<strong>Doubtnut whatsapp par inn sab ka video solution hai</strong> 🤩📝",
    "Mujhse toh ye Question nahi hone wala.❗❗❗ 😫\n\nArre aise kaise nahi hoga, <strong>Photo bhejiye📲 Doubtnut ke best teacher👩‍🏫👨‍🏫s jab samjhaege toh har question hoga</strong> 😀❗",
    "<h6>Mujh par ek ehsaan karna</h6>\n<strong>Ki Apne saare Doubts solve karte rehna</strong>",
    "<h6>Picture abhi baki hai mere dost</h6> ❗❗\n\nKya tumhare doubts bhi baki hai❓\nAgar haan toh <strong>photo bhejo solution mai dunga</strong>",
    "<h6>Doubtnut naam toh suna hi hoga</h6> ❗❗\n\n<strong>Har Doubt 10 second mein solve kar deta hai whatsapp par hi<strong>",
    "<h6>App ki jagah nahi hai phone par❓</h6>\n\nKoi nahi <strong>Doubtnut ab Whatsapp</strong> par bhi hai.\nHar Doubt ko 10 second mein solve karta hai whatsapp par hi",
    "<h6>FREE❗❗❗ FREE❗❗ FREEEEEEE</h6>\n\n\n<strong>Har Doubt ka video solution</strong> milega sirf 10 second mein woh bhi bilkul 🆓",
    "<h6>Aapki padhai hai humari zimadari</h6>\n\nBas bhejo apne doubts ki photo aur paao <strong>10 second mein video solution bikul 🆓 </strong>",
];
const smsPayload = {
    footer: "Neeche Ask Question button ko click kijiye aur doubt puchiye",
    // mediaUrl: _.sample([`${config.staticCDN}images/2022/03/28/16-07-08-308-PM_e1.jpg`, `${config.staticCDN}images/2022/03/28/16-07-22-595-PM_e2.jpg`, `${config.staticCDN}images/2022/03/28/16-07-35-668-PM_e3.jpg`]),
    replyType: "BUTTONS",
    action: {
        buttons: [
            { type: "reply", reply: { id: "1", title: "Ask a Question" } },
            { type: "reply", reply: { id: "2", title: "Home" } },
        ],
    },
};
const db = "whatsappdb";
const dbSession = "whatsapp_sessions";

function delay(ms = 10000) {
    return new Promise((resolve) => {
        setTimeout(() => {
            resolve();
        }, ms);
    });
}

async function sendFlowSms(student, index) {
    try {
        const text = msgs[index].replace("{{user_name}}", student.name || "Student");
        console.log("##student: ", student.phone, "\nname: ", student.name, "\nsuccess: ", success);
        // await whatsapp.sendTextMsg(CAMPAIGN, student.phone, 0, text, null, null, smsPayload.footer, smsPayload.replyType, smsPayload.action);
        whatsapp.sendMediaMsg(CAMPAIGN, student.phone, 0, smsPayload.mediaUrl, "IMAGE", text, smsPayload.replyType, smsPayload.action, smsPayload.footer);
        success++;
    } catch (e) {
        console.error(e);
        fails++;
    }
}

async function getBannersDnProperty(bucket, name) {
    const sql = "select value from classzoo1.dn_property where bucket = ? and name = ? and is_active =1";
    console.log(sql);
    return mysql.pool.query(sql, [bucket, name]).then((res) => res[0]);
}

async function getStudentDetails(client, lowerLimit, upperLimit) {
    const pipeline = [
        {
            $match: {
                updatedAt: { $gt: lowerLimit, $lte: upperLimit },
                source: "8400400400",
            },
        },
        {
            $project: {
                _id: 0,
                phone: 1,
                name: 1,
            },
        },
    ];
    const index = Math.floor(Math.random()*msgs.length);
    console.log("##index: ", index);
    await new Promise((resolve) => client.collection(dbSession).aggregate(pipeline, { cursor: { batchSize: 50 } }).forEach((student) => {
        sendFlowSms(student, index);
    }, (err) => {
        console.error(err);
        resolve();
    }));
    await delay(30000);
}

async function start(job) {
    const client = (await getMongoClient()).db(db);
    success = 0; fails = 0;
    let lowerLimit = new Date();
    lowerLimit.setDate(lowerLimit.getDate() - 1);
    let upperLimit = new Date();
    upperLimit.setDate(upperLimit.getDate() - 1);
    const bucketImages = await getBannersDnProperty("wa_bulk_notification", "banners");
    const bannerImage = _.sample(bucketImages[0].value.split("||"));
    smsPayload.mediaUrl = bannerImage ? `${config.staticCDN}images/${bannerImage}` : `${config.staticCDN}images/2022/03/28/16-07-08-308-PM_e1.jpg`;
    for (let i = 0; i < 6; i++) {
        upperLimit.setHours(lowerLimit.getHours() + 4);
        // console.log("loop run: ", i+1, "lowerLimit ", lowerLimit, "upperLimit ", upperLimit);
        await getStudentDetails(client, lowerLimit, upperLimit);
        lowerLimit.setHours(lowerLimit.getHours() + 4);
    }
    const cronName = "QA_PROMO_CRON";
    const cronRunDate = moment().add(5, "hours").add(30, "minutes").format("YYYY-MM-DD HH:mm:ss");
    await gupshup.sendSms({
        phone: 9804980804,
        msg: `Doubtnut Cron\n\nCron_name--${cronName}\nRun_date-${cronRunDate}\nUser count-${success}\nCron_start_time-${cronRunDate}`,
    });
    await gupshup.sendSms({
        phone: 8588829810,
        msg: `Doubtnut Cron\n\nCron_name--${cronName}\nRun_date-${cronRunDate}\nUser count-${success}\nCron_start_time-${cronRunDate}`,
    });
    await job.progress(100);
}

module.exports.start = start;
module.exports.opts = {
    cron: "03 11 * * *",
};
// mysql, date, restusers
