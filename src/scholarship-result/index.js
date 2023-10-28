/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const moment = require("moment");
const axios = require("axios");
const { mysql, sendgridMail, slack, config } = require("../../modules");
const { redshift } = require("../../modules");

async function getAllDetailsRegisteredStudents(testId, date) {
    const sql = `select t1.*, t2.time_taken,t2.min_time,t2.max_time from (SELECT distinct test_id,student_id ,eligiblescore,totalmarks, totalscore ,correct,
        incorrect,skipped
        from classzoo1.testseries_student_reportcards
        where test_id in (${testId}) and date(created_at)='${date}'
        ) as t1
        left join
        (
        SELECT distinct test_id, student_id, test_subscription_id, datediff(second,min(created_on),max(created_on)) as time_taken,min(created_on) as min_time,max(created_on) as max_time
        from classzoo1.testseries_student_responses where test_id in (${testId})
        and is_eligible=1 group by 1,2,3) as t2
        on t1.test_id=t2.test_id and t1.student_id=t2.student_id
        join classzoo1.scholarship_test st
        on st.test_id=t1.test_id and st.student_id=t1.student_id`;
    const users = await redshift.query(sql).then((res) => res);
    return users;
}

async function getScholarshipDetails() {
    const sql = "select * from scholarship_exam where is_active = 1";
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function setScholarshipResult(rank, timeTaken, couponCode, discountPercent, studentId, testId, eligiblescore) {
    const sql = `update scholarship_test set rank = '${rank}', time_taken = '${timeTaken}', coupon_code = '${couponCode}', discount_percent = '${discountPercent}', marks = ${eligiblescore} where student_id = ${studentId} and test_id = ${testId}`;
    return mysql.writePool.query(sql).then((res) => res[0]);
}

async function getCouponsRanks(testIds) {
    const sql = `select * from scholarship_coupons where test_id = '${testIds}'`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

function between(rank, min, max) {
    return rank >= min && rank <= max;
}

async function updateTestStatus(scholarshipArray) {
    const sql = `update scholarship_test set progress_id = 4 where test_id in (${scholarshipArray})`;
    return mysql.writePool.query(sql).then((res) => res[0]);
}

async function getNameAndValueByBucket(bucket) {
    const sql = "select name, value from dn_property where bucket = ? and is_active = 1";
    return mysql.pool.query(sql, [bucket]).then((res) => res[0]);
}

async function updateCouponsEndDate(endDate) {
    const validitySql = "UPDATE coupons_new set is_active=1, end_date=? WHERE description in ('SSC_DNST','DNST2','NDADNST', 'DNST2', 'DNST_NKC', 'DNST_NKC_IIT24', 'DNST_IIT24', 'DNST_NKC_NEET23', 'DNST_NKC_NEET24', 'DNST_NEET24')";
    return mysql.writePool.query(validitySql, [endDate]).then((res) => res[0]);
}

async function updateCouponLimits(limit, discountValue) {
    const sql = "UPDATE coupons_new set claim_limit=claim_limit+? WHERE description in ('SSC_DNST','DNST2','NDADNST', 'DNST2', 'DNST_NKC', 'DNST_NKC_IIT24', 'DNST_IIT24', 'DNST_NKC_NEET23', 'DNST_NKC_NEET24', 'DNST_NEET24') and value=?";
    return mysql.writePool.query(sql, [limit, discountValue]).then((res) => res[0]);
}

async function updateTgSQL(obj) {
    const sql = "update classzoo1.target_group tg set tg.sql = 'SELECT DISTINCT student_id FROM scholarship_test where discount_percent=? and test_id in (?,?) and progress_id=4 and is_active=1' where id = ?";
    return mysql.writePool.query(sql, [obj.discount, obj.test_id1, obj.test_id2, obj.id]).then((res) => res[0]);
}

async function updateInactiveTestStatus(activeTests) {
    const sql = "update scholarship_test set is_active = 0 where test_id not in (?)";
    return mysql.writePool.query(sql, [activeTests]).then((res) => res[0]);
}

async function getCouponsFromTable(testId) {
    // eslint-disable-next-line no-useless-escape
    const sql = "select * from scholarship_coupons where test_id like \'%?%\'";
    return mysql.pool.query(sql, [testId]).then((res) => res[0]);
}

async function UpdateTGAndCouponLimits(tgUpdateDetails, tgCouponLimits, tgCouponValidity) {
    const fromEmail = "autobot@doubtnut.com";
    const toEmail = "prashant.gupta@doubtnut.com";
    const ccList = ["dipankar@doubtnut.com", "aditya.mishra@doubtnut.com", "yash.bansal@doubtnut.com"];
    try {
        const couponEndDate = moment(tgCouponValidity[0].value).format("YYYY-MM-DD HH:mm:ss");
        console.log(tgCouponLimits, tgCouponValidity);
        await updateCouponsEndDate(couponEndDate);
        for (let i = 0; i < tgCouponLimits.length; i++) {
            await updateCouponLimits(tgCouponLimits[i].value, tgCouponLimits[i].name);
        }
    } catch (e) {
        console.log(e);
        await sendgridMail.sendMail(fromEmail, toEmail, "DNST Coupon Limit Alert!!!", `Coupon Limit increase Failed\n${JSON.stringify(e)}`, [], ccList);
    }
    try {
        const tgUpdatePromises = [];
        console.log(tgUpdateDetails);
        for (let i = 0; i < tgUpdateDetails.length; i++) {
            tgUpdatePromises.push(updateTgSQL({
                id: tgUpdateDetails[i].name,
                discount: JSON.parse(tgUpdateDetails[i].value)[0],
                test_id1: JSON.parse(tgUpdateDetails[i].value)[1],
                test_id2: JSON.parse(tgUpdateDetails[i].value)[2],
            }));
        }
        await Promise.all(tgUpdatePromises);
    } catch (e) {
        console.log(e);
        await sendgridMail.sendMail(fromEmail, toEmail, "DNST TG Update Alert!!!", `TG update Failed\n${JSON.stringify(e)}`, [], ccList);
    }
    return true;
}

async function start(job) {
    try {
        let startProcess = false;
        let startProcess2 = false;
        let scholarshipDetails = await getScholarshipDetails();
        const activeTests = { ...scholarshipDetails };
        console.log(moment().format());
        console.log(moment().add(5, "hours").add(30, "minutes").format());
        console.log(moment(scholarshipDetails[0].result_time).format());
        console.log(moment(scholarshipDetails[0].result_time).add(5, "hours").add(30, "minutes").format());
        const scholarshipArray = [];
        let date;
        // check if the result has been declared
        if (scholarshipDetails && scholarshipDetails[0]) {
            for (let i = 0; i < scholarshipDetails.length; i++) {
                const scholarshipDate = moment(scholarshipDetails[i].result_time).subtract(40, "minutes").format();
                const scholarshipDate2 = moment(scholarshipDetails[i].result_time).subtract(25, "minutes").format();
                const scholarshipDate3 = moment(scholarshipDetails[i].result_time).subtract(10, "minutes").format();
                const scholarshipDate4 = moment(scholarshipDetails[i].result_time).add(5, "minutes").format();
                if (moment().add(5, "hours").add(30, "minutes").isAfter(scholarshipDate) && moment().add(5, "hours").add(30, "minutes").isBefore(scholarshipDate2)) {
                    startProcess = true;
                    scholarshipArray.push(scholarshipDetails[i].test_id);
                    date = moment(scholarshipDetails[i].publish_time).format("YYYY-MM-DD");
                } else if (moment().add(5, "hours").add(30, "minutes").isAfter(scholarshipDate3) && moment().add(5, "hours").add(30, "minutes").isBefore(scholarshipDate4)) {
                    startProcess2 = true;
                    scholarshipArray.push(scholarshipDetails[i].test_id);
                    date = moment(scholarshipDetails[i].publish_time).format("YYYY-MM-DD");
                }
            }
        }
        scholarshipDetails = scholarshipDetails.filter((item) => scholarshipArray.includes(item.test_id));
        const leaderBoardTestIds = [];
        for (let i = 0; i < scholarshipDetails.length; i++) {
            if (scholarshipDetails[i].other_result_tests !== "" && scholarshipDetails[i].other_result_tests !== null && !leaderBoardTestIds.includes(scholarshipDetails[i].other_result_tests)) {
                leaderBoardTestIds.push(scholarshipDetails[i].other_result_tests);
            } else if (!leaderBoardTestIds.some((el) => el.includes(`${scholarshipDetails[i].test_id}`) && !leaderBoardTestIds.includes(`${scholarshipDetails[i].test_id}`))) {
                leaderBoardTestIds.push(`${scholarshipDetails[i].test_id}`);
            }
        }
        if (startProcess) {
            // get the details of all the registered students and tests sections of scholarship tests
            let studentsDetails = [];
            let tgUpdateDetails = [];
            let tgCouponLimits = [];
            let tgCouponValidity = [];
            [studentsDetails, tgUpdateDetails, tgCouponLimits, tgCouponValidity] = await Promise.all([
                getAllDetailsRegisteredStudents(scholarshipArray, date),
                getNameAndValueByBucket("scholarship_test_tg"),
                getNameAndValueByBucket("scholarship_test_coupon_limit"),
                getNameAndValueByBucket("scholarship_test_coupon_validity"),
            ]);
            studentsDetails.forEach((item) => {
                if (item.time_taken === null) {
                    item.time_taken = "0";
                }
                if (item.incorrect === null) {
                    item.incorrect = "0";
                }
                if (leaderBoardTestIds.some((el) => el.includes(item.test_id))) {
                    const index = leaderBoardTestIds.map((el) => el.includes(item.test_id)).indexOf(true);
                    item.testclubbed = leaderBoardTestIds[index];
                }
            });
            studentsDetails = studentsDetails.filter((thing, index, self) => index === self.findIndex((t) => (
                t.test_id === thing.test_id && t.student_id === thing.student_id
            )));
            const studentsDetailsGroup = _.groupBy(studentsDetails, "testclubbed");
            for (const key in studentsDetailsGroup) {
                if (studentsDetailsGroup[key]) {
                    const maxtime = new Date(Math.max(...studentsDetailsGroup[key].map((e) => new Date(e.max_time))));
                    studentsDetailsGroup[key].forEach((item) => {
                        if (item.min_time === null) {
                            item.min_time = maxtime;
                        }
                    });
                    // rank by more eligible score
                    const sortedNew = studentsDetailsGroup[key].sort((a, b) => b.eligiblescore - a.eligiblescore);
                    for (let i = 0; i < studentsDetailsGroup[key].length; i++) {
                        const index = sortedNew.map((e) => e.eligiblescore).indexOf(studentsDetailsGroup[key][i].eligiblescore);
                        studentsDetailsGroup[key][i].rank = parseInt(index) + 1;
                    }
                    // rank by less time taken
                    const groupedScore = _.groupBy(studentsDetailsGroup[key], "rank");
                    const newTemp = [];
                    for (const key2 in groupedScore) {
                        if (groupedScore[key2].length > 1) {
                            const rank2 = key2;
                            const sortedNew2 = groupedScore[key2].sort((a, b) => a.time_taken - b.time_taken);
                            for (let i = 0; i < groupedScore[key2].length; i++) {
                                const index = sortedNew2.map((e) => e.time_taken).indexOf(groupedScore[key2][i].time_taken);
                                groupedScore[key2][i].rank = parseInt(rank2) + index;
                                newTemp.push(groupedScore[key2][i]);
                            }
                        }
                    }
                    studentsDetailsGroup[key].forEach((item) => {
                        if (newTemp.some((el) => el.student_id === item.student_id)) {
                            const index = newTemp.map((e) => e.student_id).indexOf(item.student_id);
                            item.rank = newTemp[index].rank;
                        }
                    });
                    studentsDetailsGroup[key].sort((a, b) => a.rank - b.rank);
                    // rank by less incorrect answers
                    const groupedScore2 = _.groupBy(studentsDetailsGroup[key], "rank");
                    const newTemp2 = [];
                    for (const key3 in groupedScore2) {
                        if (groupedScore2[key3].length > 1) {
                            const rank3 = key3;
                            const sortedNew3 = groupedScore2[key3].sort((a, b) => a.incorrect.split(",").length - b.incorrect.split(",").length);
                            for (let i = 0; i < groupedScore2[key3].length; i++) {
                                const index = sortedNew3.map((e) => e.incorrect.split(",").length).indexOf(groupedScore2[key3][i].incorrect.split(",").length);
                                groupedScore2[key3][i].rank = parseInt(rank3) + index;
                                newTemp2.push(groupedScore2[key3][i]);
                            }
                        }
                    }
                    studentsDetailsGroup[key].forEach((item) => {
                        if (newTemp2.some((el) => el.student_id === item.student_id)) {
                            const index = newTemp2.map((e) => e.student_id).indexOf(item.student_id);
                            item.rank = newTemp2[index].rank;
                        }
                    });
                    studentsDetailsGroup[key].sort((a, b) => a.rank - b.rank);
                    // rank by earliest started
                    const groupedScore3 = _.groupBy(studentsDetailsGroup[key], "rank");
                    const newTemp3 = [];
                    for (const key4 in groupedScore3) {
                        if (groupedScore3[key4].length > 1) {
                            const rank4 = key4;
                            const sortedNew3 = groupedScore3[key4].sort((a, b) => moment(a.min_time).diff(moment()) - moment(b.min_time).diff(moment()));
                            for (let i = 0; i < groupedScore3[key4].length; i++) {
                                const index = sortedNew3.map((e) => e.min_time).indexOf(groupedScore3[key4][i].min_time);
                                groupedScore3[key4][i].rank = parseInt(rank4) + index;
                                newTemp3.push(groupedScore3[key4][i]);
                            }
                        }
                    }
                    studentsDetailsGroup[key].forEach((item) => {
                        if (newTemp3.some((el) => el.student_id === item.student_id)) {
                            const index = newTemp3.map((e) => e.student_id).indexOf(item.student_id);
                            item.rank = newTemp3[index].rank;
                        }
                    });
                    // const groupedScore4 = _.groupBy(studentsDetailsGroup[key], "rank");
                    const testIds = key.replace("||", ",");
                    const getCoupons = await getCouponsRanks(testIds);
                    const rangeArray = [];
                    const couponsArray = [];
                    const discountArray = [];
                    getCoupons.forEach((item) => {
                        rangeArray.push(item.rank_range);
                        couponsArray.push(item.coupon_code);
                        discountArray.push(item.discount);
                    });
                    const chunk = 100;
                    for (let e = 0, f = studentsDetailsGroup[key].length; e < f; e += chunk) {
                        const slice = studentsDetailsGroup[key].slice(e, e + chunk);
                        const workers = [];
                        for (let i = 0; i < slice.length; i++) {
                            let index;
                            for (let j = 0; j < rangeArray.length; j++) {
                                const min = rangeArray[j].split(",")[0];
                                let max = rangeArray[j].split(",")[1];
                                if (!max) {
                                    max = min;
                                }
                                if (between(slice[i].rank, min, max)) {
                                    index = j;
                                    break;
                                }
                            }
                            workers.push(setScholarshipResult(slice[i].rank, slice[i].time_taken, couponsArray[index], discountArray[index], slice[i].student_id, slice[i].test_id, slice[i].eligiblescore));
                        }
                        await Promise.all(workers);
                    }
                }
            }
            await UpdateTGAndCouponLimits(tgUpdateDetails, tgCouponLimits, tgCouponValidity);
            const blockNew = [];
            blockNew.push({
                type: "section",
                text: { type: "mrkdwn", text: `Number of students results updated on ${date}` },
            },
            {
                type: "section",
                text: { type: "mrkdwn", text: `*Count*: ${studentsDetails.length}` },
            });
            await slack.sendMessage("#scholarship-notification", blockNew, config.SCHOLARSHIP_SLACK_AUTH);
        }
        if (startProcess2) {
            await updateTestStatus(scholarshipArray);
            const activeScholarshipTests = [];
            _.forEach(activeTests, (item) => {
                activeScholarshipTests.push(item.test_id);
            });
            await updateInactiveTestStatus(activeScholarshipTests);
            const workers = [];
            for (let i = 0; i < scholarshipArray.length; i++) {
                workers.push(getCouponsFromTable(scholarshipArray[i]));
            }
            let coupons = await Promise.all(workers);
            coupons = _.flatten(coupons);
            coupons = _.uniqBy(coupons, "coupon_code");
            const couponList = [];
            for (let i = 0; i < coupons.length; i++) {
                couponList.push(coupons[i].coupon_code);
            }
            // Coupon Codes to be passed as a string seperated by ,
            const options = {
                method: "POST",
                url: "https://panel-api.doubtnut.com/v1/coupon/update-coupon-index",
                data: {
                    coupon_codes: couponList.join(','),
                },
            };
            const { data } = await axios(options);
            console.log(data);
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
    removeOnComplete: 10,
    removeOnFail: 10,
};
