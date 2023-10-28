/* eslint-disable no-await-in-loop */
/* eslint-disable no-lonely-if */
const moment = require("moment");
const { config, mysql, notification } = require("../../modules");
const { redshift, slack } = require("../../modules");

async function getAllDetailsRegisteredStudents(testId) {
    const sql = `select b.student_id, b.gcm_reg_id, b.locale, a.rank, a.discount_percent ,a.coupon_code, a.test_id from classzoo1.scholarship_test a inner join classzoo1.students b on a.student_id = b.student_id where a.is_active = 1 and a.test_id in (${testId}) and b.gcm_reg_id is not null`;
    const users = await redshift.query(sql).then((res) => res);
    return users;
}

async function getScholarshipDetails() {
    const sql = "select * from scholarship_exam where is_active = 1 and notification_time is not null";
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getAllBanners(typeBanner) {
    const sql = "select * from scholarship_banners where is_active = 1 and type in (?)";
    return mysql.pool.query(sql, [typeBanner]).then((res) => res[0]);
}

let stickyNumber;

function getNotificationPayload(locale, notificationNumber, date1, date, rank, discountPercent, couponCode, isTalent, scholarshipDetails, studentBanner, studentTestId, startBanner) {
    let notifTitle;
    let notifMessage;
    let snId = "";
    if (notificationNumber === 1) {
        if (isTalent) {
            notifTitle = locale === "hi" ? "आ गया है Doubtnut सुपर 100 टेस्ट का दिन" : "Aagya hai Doubtnut Super 100 test ka din";
            notifMessage = locale === "hi" ? `क्या आप सब है तैयार?\nटेस्ट का समय - ${date1}` : `Kya aap sab hai taiyaar?\nTest time - ${date1}`;
        } else if (scholarshipDetails[0].type.includes("NKC")) {
            notifTitle = "The day for Scholarship test is here!";
            notifMessage = `Are you Ready?\nTest Time – ${date1}`;
        } else {
            notifTitle = locale === "hi" ? "आ गया है स्कॉलरशिप टेस्ट का दिन" : "Aagya hai scholarship test ka din";
            notifMessage = locale === "hi" ? `क्या आप सब है तैयार?\nटेस्ट का समय - ${date1}` : `Kya aap sab hai taiyaar?\nTest time - ${date1}`;
        }
    } else if (notificationNumber === 2) {
        if (scholarshipDetails[0].type.includes("NKC")) {
            notifTitle = "Roll up your sleeves and get ready my friend!";
            notifMessage = "Only 2 hours are left for the Scholarship test";
        } else {
            notifTitle = locale === "hi" ? "कमर कस लें हो जाएं तैयार" : "Kamar kas le ho jaayein taiyaar";
            notifMessage = locale === "hi" ? "बस 2 घंटे बचे हैं स्कॉलरशिप टेस्ट में मेरे यार!" : "Bas 2 ghante bache hain scholarship test mein mere yaar!";
        }
    } else if (notificationNumber === 3) {
        if (isTalent) {
            notifTitle = locale === "hi" ? "जल्दी हो जाओ फ्री गुरु !" : "Jaldi ho jao free guru !";
            notifMessage = locale === "hi" ? "आपका Doubtnut सुपर 100 टेस्ट 1 घंटे में है शुरू ।" : "Aapka Doubtnut Super 100 test hai 1 ghante mein hai shuru";
        } else if (scholarshipDetails[0].type.includes("NKC")) {
            notifTitle = "Hurry up! Get ready to show your power";
            notifMessage = "Your Scholarship Test starts in 1 hour";
        } else {
            notifTitle = locale === "hi" ? "जल्दी हो जाओ फ्री गुरु !" : "Jaldi ho jao free guru !";
            notifMessage = locale === "hi" ? "आपका स्कॉलरशिप टेस्ट 1 घंटे में है शुरू ।" : "Aapka scholarship test hai 1 ghante mein hai shuru";
        }
    } else if (notificationNumber === 4) {
        if (isTalent) {
            notifTitle = locale === "hi" ? "सुनो! सुनो! सुनो!" : "Suno! Suno! Suno!";
            notifMessage = locale === "hi" ? "आपका Doubtnut सुपर 100 टेस्ट शुरू होने वाला है...जल्दी ज्वॉइन करें! ऑल द बेस्ट !" : "Aapka Doubtnut Super 100 test shuru hone vala hai..Jaldi join karein! All the best";
        } else if (scholarshipDetails[0].type.includes("NKC")) {
            notifTitle = "Listen! Listen! Listen!";
            notifMessage = "Your Scholarship Test is about to start…Hurry Join! All The Best";
        } else {
            notifTitle = locale === "hi" ? "सुनो! सुनो! सुनो!" : "Suno! Suno! Suno!";
            notifMessage = locale === "hi" ? "आपका स्कॉलरशिप टेस्ट शुरू होने वाला है...जल्दी ज्वॉइन करें! ऑल द बेस्ट !" : "Aapka scholarship test shuru hone vala hai..Jaldi join karein! All the best";
        }
    } else if (notificationNumber === 5) {
        if (isTalent) {
            notifTitle = locale === "hi" ? "सुनो! सुनो! सुनो!" : "Suno! Suno! Suno!";
            notifMessage = locale === "hi" ? "आपका Doubtnut सुपर 100 टेस्ट शुरू हो चुका है...जल्दी ज्वॉइन करें! ऑल द बेस्ट !" : "Aapka Doubtnut Super 100 test shuru ho chuka hai..Jaldi join karein! All the best";
        } else if (scholarshipDetails[0].type.includes("NKC")) {
            notifTitle = "Listen! Listen! Listen!";
            notifMessage = "Your Scholarship Test is about to start…Hurry Join! All The Best";
        } else {
            notifTitle = locale === "hi" ? "सुनो! सुनो! सुनो!" : "Suno! Suno! Suno!";
            notifMessage = locale === "hi" ? "आपका स्कॉलरशिप टेस्ट शुरू हो चुका है...जल्दी ज्वॉइन करें! ऑल द बेस्ट !" : "Aapka scholarship test shuru ho chuka hai..Jaldi join karein! All the best";
        }
        snId = "DNST_start_test";
    } else if (notificationNumber === 6) {
        if (isTalent) {
            notifTitle = locale === "hi" ? "सुनो! सुनो! सुनो!" : "Suno! Suno! Suno!";
            notifMessage = locale === "hi" ? "आपका Doubtnut सुपर 100 टेस्ट शुरू हो चुका है...जल्दी ज्वॉइन करें! ऑल द बेस्ट !" : "Aapka Doubtnut Super 100 test shuru ho chuka hai..Jaldi join karein! All the best";
        } else if (scholarshipDetails[0].type.includes("NKC")) {
            notifTitle = "Listen! Listen! Listen!";
            notifMessage = "Your Scholarship Test is about to start…Hurry Join! All The Best";
        } else {
            notifTitle = locale === "hi" ? "सुनो! सुनो! सुनो!" : "Suno! Suno! Suno!";
            notifMessage = locale === "hi" ? "आपका स्कॉलरशिप टेस्ट शुरू हो चुका है...जल्दी ज्वॉइन करें! ऑल द बेस्ट !" : "Aapka scholarship test shuru ho chuka hai..Jaldi join karein! All the best";
        }
    } else if (notificationNumber === 7) {
        if (isTalent) {
            notifTitle = locale === "hi" ? `Doubtnut सुपर 100 टेस्ट का परिणाम ${date} घोषित किया जाएगा ।` : `Doubtnut Super 100 test ka result ${date} declare hoga !`;
            notifMessage = locale === "hi" ? "अभी जवाब कुंजी देखें" : "Check Answer key now";
        } else {
            notifTitle = locale === "hi" ? `स्कॉलरशिप टेस्ट का परिणाम ${date} घोषित किया जाएगा ।` : `Scholarship test ka result ${date} declare hoga !`;
            notifMessage = locale === "hi" ? "अभी जवाब कुंजी देखें" : "Check Answer key now";
        }
        snId = "DNST_answer_key";
    } else if (notificationNumber === 8) {
        if (isTalent) {
            notifTitle = locale === "hi" ? "खत्म हुआ इंतजार आ गया है Doubtnut सुपर 100 का रिजल्ट !" : "Khatam hua intezaar aa gya hai Doubtnut Super 100 ka result !";
            notifMessage = locale === "hi" ? "अभी देखें" : "Check now";
        } else if (scholarshipDetails[0].type.includes("NKC")) {
            notifTitle = "The wait is over, the result of the DNST has come!";
            notifMessage = "Check Now";
        } else {
            notifTitle = locale === "hi" ? "खत्म हुआ इंतजार आ गया है DNST का रिजल्ट !" : "Khatam hua intezaar aa gya hai DNST ka result !";
            notifMessage = locale === "hi" ? "अभी देखें" : "Check now";
        }
        snId = "DNST_result_announcement";
    } else if (notificationNumber > 8 && !isTalent) {
        if (rank !== null) {
            if (scholarshipDetails[0].type.includes("NKC")) {
                notifTitle = `Congratulations in securing ${rank} rank in the Scholarship Test`;
            } else {
                notifTitle = locale === "hi" ? `स्कॉलरशिप टेस्ट में  ${rank} रैंक लाने पर बधाई` : `Scholarship Test main ${rank} rank lane par badhai`;
            }
        } else {
            if (scholarshipDetails[0].type.includes("NKC")) {
                notifTitle = "Congratulations on participating in the Scholarship Test";
            } else {
                notifTitle = locale === "hi" ? "स्कॉलरशिप टेस्ट में भाग लेने पर बधाई" : "Scholarship Test main participate karne par badhai";
            }
        }
        if (scholarshipDetails[0].type.includes("NKC")) {
            notifMessage = `Buy Courses on Doubtnut and avail Scholarships of ${discountPercent}% \nCoupon Code = ${couponCode}`;
        } else {
            notifMessage = locale === "hi" ? `Doubtnut पर कोर्स खरीदें और ${discountPercent}% की स्कॉलरशिप का फायदा उठाएं Coupon Code = ${couponCode}` : `Doubtnut par course khareedein aur ${discountPercent}% ki scholarship ka fayda uthae Coupon Code = ${couponCode}`;
        }
    }
    let notificationPayload;
    if (notificationNumber < 9) {
        const startOfDay = moment().startOf("day");
        let isSticky = false;
        const dateToday = moment().add(5, "hours").add(30, "minutes").format("YYYY-MM-DD");
        if (moment().add(5, "hours").add(30, "minutes").isAfter(moment(startOfDay).add(13, "hours").add(55, "minutes")) && moment().add(5, "hours").add(30, "minutes").isBefore(moment(startOfDay).add(15, "hours").add(40, "minutes"))) {
            isSticky = true;
            stickyNumber = 1;
        }
        if (moment().add(5, "hours").add(30, "minutes").isAfter(moment(startOfDay).add(14, "hours").add(25, "minutes")) && moment().add(5, "hours").add(30, "minutes").isBefore(moment(startOfDay).add(14, "hours").add(35, "minutes"))) {
            isSticky = true;
            stickyNumber = 2;
        }
        if (moment().add(5, "hours").add(30, "minutes").isAfter(moment(startOfDay).add(14, "hours").add(55, "minutes")) && moment().add(5, "hours").add(30, "minutes").isBefore(moment(startOfDay).add(15, "hours").add(5, "minutes"))) {
            isSticky = true;
            stickyNumber = 3;
        }
        if (moment().add(5, "hours").add(30, "minutes").isAfter(moment(startOfDay).add(15, "hours").add(25, "minutes")) && moment().add(5, "hours").add(30, "minutes").isBefore(moment(startOfDay).add(15, "hours").add(35, "minutes"))) {
            isSticky = true;
            stickyNumber = 4;
        }
        let event = "course_details";
        let id = `scholarship_test_${scholarshipDetails[0].type}`;
        if ([4, 5, 6].includes(notificationNumber)) {
            event = "mock_test_subscribe";
            id = `${studentTestId}`;
        }
        notificationPayload = {
            event,
            title: notifTitle,
            message: notifMessage,
            s_n_id: snId !== "" ? snId : `Dnst_reminder_${notificationNumber}`,
            firebase_eventtag: "dnst_scholarship",
            data: JSON.stringify({
                id,
            }),
        };
        if (notificationNumber == 5 && isSticky) {
            notificationPayload = {
                event: "course_notification",
                firebase_eventtag: "course_notification",
                sn_type: "banner",
                s_n_id: `Dnst_${stickyNumber}_start_sticky_${dateToday}`,
                title: notifTitle,
                message: notifMessage,
                data: {
                    id: 6540,
                    image_url: startBanner[0].url,
                    is_vanish: true,
                    deeplink_banner: `doubtnutapp://mock_test_subscribe?id=${id}`,
                    offset: 1800 * 1000,
                },
            };
        }
    } else {
        let filter;
        if (couponCode.includes("C6")) {
            filter = `6,$$${couponCode}`;
        } else if (couponCode.includes("C7")) {
            filter = `7,$$${couponCode}`;
        } else if (couponCode.includes("C8")) {
            filter = `8,$$${couponCode}`;
        } else if (couponCode.includes("C9")) {
            filter = `9,$$${couponCode}`;
        } else if (couponCode.includes("C10")) {
            filter = `10,$$${couponCode}`;
        } else if (couponCode.includes("C11")) {
            filter = `11,$$${couponCode}`;
        } else if (couponCode.includes("C12B")) {
            filter = `12,$$${couponCode}`;
        } else if (couponCode.includes("C12M")) {
            filter = `12,$$${couponCode}`;
        } else if (couponCode.includes("I22")) {
            filter = `IIT JEE_CT,12,2022,$$${couponCode}`;
        } else if (couponCode.includes("I23")) {
            filter = `IIT JEE_CT,11,2023,$$${couponCode}`;
        } else if (couponCode.includes("N22")) {
            filter = `NEET_CT,12,2022,$$${couponCode}`;
        } else if (couponCode.includes("N23")) {
            filter = `NEET_CT,11,2023,$$${couponCode}`;
        } else if (couponCode.includes("NDA")) {
            filter = `DEFENCE/NDA/NAVY_CT,12,$$${couponCode}`;
        }
        const dateToday = moment().add(5, "hours").add(30, "minutes").format("YYYY-MM-DD");
        if (notificationNumber === 10) {
            let deeplinkBanner = `doubtnutapp://course_category?title=Apke%20liye%20Courses&filters=${filter}`;
            if (scholarshipDetails[0].type.includes("NKC")) {
                deeplinkBanner = `doubtnutapp://course_details?id=${scholarshipDetails[0].assortment_ids.split("||")[0]}`;
            }
            notificationPayload = {
                event: "course_notification",
                firebase_eventtag: "course_notification",
                sn_type: "text",
                s_n_id: `Dnst_sales_sticky_${dateToday}`,
                title: notifTitle,
                message: notifMessage,
                data: {
                    id: 6539,
                    image_url: `${config.staticCDN}engagement_framework/170A60A2-62D4-FB41-459B-9A64D2F2DE15.webp`,
                    is_vanish: true,
                    deeplink_banner: deeplinkBanner,
                    offset: 10800 * 1000,
                },
            };
        } else {
            let event = "course_category";
            let data = JSON.stringify({
                title: "Apke liye Courses",
                filters: filter,
            });
            if (scholarshipDetails[0].type.includes("NKC")) {
                event = "course_details";
                data = JSON.stringify({
                    id: scholarshipDetails[0].assortment_ids.split("||")[0],
                });
            }
            notificationPayload = {
                event,
                title: notifTitle,
                message: notifMessage,
                s_n_id: `Dnst_sales_${dateToday}`,
                image: studentBanner[0].url,
                firebase_eventtag: "dnst_scholarship_result",
                data,
            };
        }
    }
    return notificationPayload;
}

async function start(job) {
    try {
        let notificationNumber = 0;
        let scholarshipDate;
        let send = false;
        let scholarshipDetails = await getScholarshipDetails();
        const scholarshipArray = [];
        const scholarshipArraySales = [];
        let date;
        let date1;
        let isTalent = false;
        let sales = false;
        let overwriteSales = false;
        if (scholarshipDetails && scholarshipDetails[0]) {
            for (let i = 0; i < scholarshipDetails.length; i++) {
                scholarshipDate = scholarshipDetails[i].notification_time;
                if (moment().add(5, "hours").add(30, "minutes").isSame(moment(scholarshipDate), "day")) {
                    send = true;
                    scholarshipArray.push(scholarshipDetails[i].test_id);
                    date1 = moment(scholarshipDetails[i].publish_time).format("M MMMM, h:mm A");
                    date = moment(scholarshipDetails[i].result_time).format("M MMMM, h:mm A");
                    if (scholarshipDetails[i].type.includes("TALENT")) {
                        isTalent = true;
                    }
                    overwriteSales = true;
                } else if ((moment().subtract(29, "hours").subtract(30, "minutes").isSame(moment(scholarshipDate), "day") || moment().subtract(53, "hours").subtract(30, "minutes").isSame(moment(scholarshipDate), "day") || moment().subtract(77, "hours").subtract(30, "minutes").isSame(moment(scholarshipDate), "day") || moment().subtract(101, "hours").subtract(30, "minutes").isSame(moment(scholarshipDate), "day") || moment().subtract(125, "hours").subtract(30, "minutes").isSame(moment(scholarshipDate), "day") || moment().subtract(149, "hours").subtract(30, "minutes").isSame(moment(scholarshipDate), "day")) && !scholarshipDetails[i].type.includes("TALENT")) {
                    send = true;
                    sales = true;
                    scholarshipArraySales.push(scholarshipDetails[i].test_id);
                }
            }
        }
        if (overwriteSales) {
            scholarshipDetails = scholarshipDetails.filter((item) => scholarshipArray.includes(item.test_id));
            sales = false;
        } else {
            scholarshipDetails = scholarshipDetails.filter((item) => scholarshipArraySales.includes(item.test_id));
        }
        if (send) {
            const startOfDay = moment().startOf("day");
            if (isTalent) {
                if (moment().add(5, "hours").add(30, "minutes").isAfter(moment(startOfDay).add(7, "hours").add(55, "minutes")) && moment().add(5, "hours").add(30, "minutes").isBefore(moment(startOfDay).add(8, "hours").add(10, "minutes"))) {
                    notificationNumber = 1;
                } else if (moment().add(5, "hours").add(30, "minutes").isAfter(moment(startOfDay).add(8, "hours").add(55, "minutes")) && moment().add(5, "hours").add(30, "minutes").isBefore(moment(startOfDay).add(9, "hours").add(10, "minutes"))) {
                    notificationNumber = 3;
                } else if (moment().add(5, "hours").add(30, "minutes").isAfter(moment(startOfDay).add(9, "hours").add(40, "minutes")) && moment().add(5, "hours").add(30, "minutes").isBefore(moment(startOfDay).add(9, "hours").add(50, "minutes"))) {
                    notificationNumber = 4;
                } else if (moment().add(5, "hours").add(30, "minutes").isAfter(moment(startOfDay).add(9, "hours").add(55, "minutes")) && moment().add(5, "hours").add(30, "minutes").isBefore(moment(startOfDay).add(10, "hours").add(5, "minutes"))) {
                    notificationNumber = 5;
                } else if (moment().add(5, "hours").add(30, "minutes").isAfter(moment(startOfDay).add(10, "hours").add(10, "minutes")) && moment().add(5, "hours").add(30, "minutes").isBefore(moment(startOfDay).add(10, "hours").add(20, "minutes"))) {
                    notificationNumber = 6;
                } else if (moment().add(5, "hours").add(30, "minutes").isAfter(moment(startOfDay).add(14, "hours").add(55, "minutes")) && moment().add(5, "hours").add(30, "minutes").isBefore(moment(startOfDay).add(15, "hours").add(10, "minutes"))) {
                    notificationNumber = 7;
                } else if (moment().add(5, "hours").add(30, "minutes").isAfter(moment(startOfDay).add(15, "hours").add(55, "minutes")) && moment().add(5, "hours").add(30, "minutes").isBefore(moment(startOfDay).add(16, "hours").add(10, "minutes"))) {
                    notificationNumber = 8;
                }
            } else {
                if (moment().add(5, "hours").add(30, "minutes").isAfter(moment(startOfDay).add(9, "hours").add(55, "minutes")) && moment().add(5, "hours").add(30, "minutes").isBefore(moment(startOfDay).add(10, "hours").add(10, "minutes")) && !sales) {
                    notificationNumber = 1;
                } else if (moment().add(5, "hours").add(30, "minutes").isAfter(moment(startOfDay).add(11, "hours").add(55, "minutes")) && moment().add(5, "hours").add(30, "minutes").isBefore(moment(startOfDay).add(12, "hours").add(10, "minutes")) && !sales) {
                    notificationNumber = 2;
                } else if (moment().add(5, "hours").add(30, "minutes").isAfter(moment(startOfDay).add(12, "hours").add(55, "minutes")) && moment().add(5, "hours").add(30, "minutes").isBefore(moment(startOfDay).add(13, "hours").add(10, "minutes")) && !sales) {
                    notificationNumber = 3;
                } else if (moment().add(5, "hours").add(30, "minutes").isAfter(moment(startOfDay).add(13, "hours").add(40, "minutes")) && moment().add(5, "hours").add(30, "minutes").isBefore(moment(startOfDay).add(13, "hours").add(50, "minutes")) && !sales) {
                    notificationNumber = 4;
                } else if (moment().add(5, "hours").add(30, "minutes").isAfter(moment(startOfDay).add(13, "hours").add(55, "minutes")) && moment().add(5, "hours").add(30, "minutes").isBefore(moment(startOfDay).add(15, "hours").add(40, "minutes")) && !sales) {
                    notificationNumber = 5;
                // eslint-disable-next-line brace-style
                }
                // else if (moment().add(5, "hours").add(30, "minutes").isAfter(moment(startOfDay).add(15, "hours").add(10, "minutes")) && moment().add(5, "hours").add(30, "minutes").isBefore(moment(startOfDay).add(15, "hours").add(20, "minutes")) && !sales) {
                //     notificationNumber = 6;
                // }
                else if (moment().add(5, "hours").add(30, "minutes").isAfter(moment(startOfDay).add(16, "hours").add(55, "minutes")) && moment().add(5, "hours").add(30, "minutes").isBefore(moment(startOfDay).add(17, "hours").add(10, "minutes")) && !sales && !scholarshipDetails[0].type.includes("NKC")) {
                    notificationNumber = 7;
                } else if (moment().add(5, "hours").add(30, "minutes").isAfter(moment(startOfDay).add(17, "hours").add(55, "minutes")) && moment().add(5, "hours").add(30, "minutes").isBefore(moment(startOfDay).add(18, "hours").add(10, "minutes")) && !sales) {
                    notificationNumber = 8;
                } else if (moment().add(5, "hours").add(30, "minutes").isAfter(moment(startOfDay).add(18, "hours").add(55, "minutes")) && moment().add(5, "hours").add(30, "minutes").isBefore(moment(startOfDay).add(19, "hours").add(10, "minutes"))) {
                    notificationNumber = 9;
                } else if (moment().add(5, "hours").add(30, "minutes").isAfter(moment(startOfDay).add(20, "hours").add(25, "minutes")) && moment().add(5, "hours").add(30, "minutes").isBefore(moment(startOfDay).add(20, "hours").add(35, "minutes"))) {
                    notificationNumber = 10;
                }
            }
        }
        if (notificationNumber !== 0) {
            let testToCheck;
            if (overwriteSales) {
                testToCheck = scholarshipArray;
            } else {
                testToCheck = scholarshipArraySales;
            }
            const studentsDetails = await getAllDetailsRegisteredStudents(testToCheck);
            let types = [];
            for (let i = 0; i < scholarshipDetails.length; i++) {
                types.push(scholarshipDetails[i].type);
            }
            types = [...new Set(types)];
            const typeBanner = [];
            const typeBanner2 = [];
            for (let i = 0; i < types.length; i++) {
                typeBanner.push(`${types[i]}_sales`);
                typeBanner2.push(`${types[i]}_start_test_sticky`);
            }
            const allBanners = await getAllBanners(typeBanner);
            const allBanners2 = await getAllBanners(typeBanner2);
            job.progress(10);
            const workers = [];
            const chunkSize = 100;
            for (let e = 0, f = studentsDetails.length; e < f; e += chunkSize) {
                const studentsSlice = studentsDetails.slice(e, e + chunkSize);
                for (let i = 0; i < studentsSlice.length; i++) {
                    if (studentsSlice[i].locale !== "hi") {
                        studentsSlice[i].locale = "en";
                    }
                }
                for (let i = 0; i < studentsSlice.length; i++) {
                    const scholarshipDetailsTemp = scholarshipDetails.filter((item) => item.test_id == studentsSlice[i].test_id);
                    const studentBanner = allBanners.filter((item) => item.coupon_code == studentsSlice[i].coupon_code && item.type.includes(scholarshipDetailsTemp[0].type) && item.locale === studentsSlice[i].locale);
                    const startBanner = allBanners2.filter((item) => item.locale == studentsSlice[i].locale && item.type.includes(scholarshipDetailsTemp[0].type) && item.locale === studentsSlice[i].locale);
                    const notificationPayload = getNotificationPayload(studentsSlice[i].locale, notificationNumber, date1, date, studentsSlice[i].rank, studentsSlice[i].discount_percent, studentsSlice[i].coupon_code, isTalent, scholarshipDetailsTemp, studentBanner, studentsSlice[i].test_id, startBanner);
                    // eslint-disable-next-line no-await-in-loop
                    workers.push(notification.sendNotification([{ id: studentsSlice[i].student_id, gcmId: studentsSlice[i].gcm_reg_id }], notificationPayload));
                }
                await Promise.all(workers);
            }
            const blockNew = [];
            blockNew.push({
                type: "section",
                text: { type: "mrkdwn", text: `Scholarship notification number "${notificationNumber}" sent` },
            },
            {
                type: "section",
                text: { type: "mrkdwn", text: `*Count*: ${studentsDetails.length}` },
            });
            if ([1, 2, 3, 4].includes(stickyNumber)) {
                blockNew.push({
                    type: "section",
                    text: { type: "mrkdwn", text: `This was sticky notification number :- "${stickyNumber}"` },
                });
            }
            await slack.sendMessage("#scholarship-notification", blockNew, config.SCHOLARSHIP_SLACK_AUTH);
        }
        job.progress(100);
        return {
            data: {
                done: true,
            },
        };
    } catch (err) {
        console.log(err);
        return { err };
    }
}
module.exports.start = start;
module.exports.opts = {
    cron: "*/15 * * * *",
};
