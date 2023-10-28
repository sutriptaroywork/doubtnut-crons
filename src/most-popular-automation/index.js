/* eslint-disable no-restricted-globals */
/* eslint-disable no-await-in-loop */
/* eslint-disable guard-for-in */
const _ = require("lodash");
const { redshift, mysql } = require("../../modules");

async function getAllResources() {
    const sql = `select distinct
    lcr.resource_reference ,lcr.subject, lcr.liveclass_course_detail_id, lcr.liveclass_course_id,
    lcd.faculty_id,lcd.master_chapter, lcd.chapter,lcd.live_at,lcd.cid,
    lc.is_free,lc.locale, lc.course_exam,lc.class,
    du.name as expert_name, du.id as expert_id,cdlcm.vendor_id, cdlcm.class_type
    from
    (
        select resource_reference,subject,liveclass_course_id,
        min(liveclass_course_detail_id) as liveclass_course_detail_id
        from
        classzoo1.liveclass_course_resources
        where resource_type in (1,4,8) and subject not in  ('ANNOUNCEMENT','GUIDANCE','TRAINING','INTRODUCTION')
        group by 1,2,3
    ) lcr
    left join
    (
        select id as detail_id,faculty_id,
        master_chapter,chapter,live_at,
        min(liveclass_course_id) as cid
        from  classzoo1.liveclass_course_details
        where is_replay  = 0  	
        group by 1,2,3,4,5
    ) lcd on lcd.detail_id =  lcr.liveclass_course_detail_id
    left join classzoo1.dashboard_users du  on du.id =  lcd.faculty_id
    left join
    (
        select id as cid, locale, course_exam,is_free, min(class) as class
        from classzoo1.liveclass_course
        group by 1,2,3,4
    ) lc on lc.cid = lcd.cid
    left join  classzoo1.course_details_liveclass_course_mapping cdlcm on cdlcm.liveclass_course_id = lc.cid
    where cdlcm.vendor_id  = 1`;
    const users = await redshift.query(sql).then((res) => res);
    return users;
}

async function getViews(questionList) {
    const sql = `select question_id, sum(engage_time) as et, count(DISTINCT student_id) as st , count(view_id) as views
    from  classzoo1.video_view_stats
    where engage_time >= 0  and engage_time <=  100000
    and view_from not in ('community')
    and question_id in (${questionList})
    group by 1`;
    const users = await redshift.query(sql).then((res) => res);
    return users;
}

async function getRating(questionList) {
    const sql = `select cr.resource_reference, sum(star_rating) as sum_rating,  count(star_rating) as ct_rating
    from classzoo1.liveclass_feedback_response lfr
    left join classzoo1.course_resources cr on cr.id = lfr.detail_id
    where cr.resource_type in (1,4,8)
    and cr.resource_reference in (${questionList})
    group by 1`;
    const users = await redshift.query(sql).then((res) => res);
    return users;
}

async function getActiveTopFreeClasses(qidList) {
    const mysqlQ = "select * from top_free_classes where is_active = 1 and question_id in (?)";
    return mysql.pool.query(mysqlQ, [qidList]).then(([res]) => res);
}

async function insertResource(obj) {
    const mysqlQ = "insert into top_free_classes SET ?";
    return mysql.writePool.query(mysqlQ, obj).then(([res]) => res);
}

async function updateResource(obj, id) {
    const mysqlQ = `update top_free_classes set is_active = 1, average_rating=${obj.avg_rating}, et_per_st=${obj.et_per_st} where question_id = ${obj.resource_reference} and id = ${id}`;
    return mysql.writePool.query(mysqlQ).then(([res]) => res);
}

async function disableOldEntries() {
    const sql = "update top_free_classes set is_active = 0";
    return mysql.writePool.query(sql);
}

async function start(job) {
    try {
        job.progress(0);
        // setp 1 & 2 - fetch data and map the video views and ratings
        let allResources = await getAllResources();
        allResources = allResources.filter((item) => item.expert_id !== null && item.master_chapter !== null && item.subject !== null && item.class !== null && item.locale !== null && item.course_exam !== null && item.class_type !== null && item.is_free !== null && item.resource_reference !== null);
        allResources = allResources.filter((item) => item.expert_id !== undefined && item.master_chapter !== undefined && item.subject !== undefined && item.class !== undefined && item.locale !== undefined && item.course_exam !== undefined && item.class_type !== undefined && item.is_free !== undefined && item.resource_reference !== undefined);
        allResources = allResources.filter((item) => !isNaN(+item.resource_reference) && item.resource_reference !== "");
        // filter duplicate items with resource_reference
        allResources = _.uniqBy(allResources, "resource_reference");
        console.log(`Total resources to process: ${allResources.length}`);
        const chunk1 = 1000;
        for (let i = 0; i < allResources.length; i += chunk1) {
            console.log(`Processing chunk ${i} to ${i + chunk1}`);
            const chunk = allResources.slice(i, i + chunk1);
            const resourceReferenceArrInt = chunk.map((item) => +item.resource_reference);
            const [rating, views] = await Promise.all([getRating(resourceReferenceArrInt), getViews(resourceReferenceArrInt)]);
            console.log(`Rating: ${rating.length}`);
            console.log(`Views: ${views.length}`);
            for (let j = 0; j < chunk.length; j++) {
                const viewIndex = _.findIndex(views, (o) => (o.question_id == chunk[j].resource_reference));
                const ratingIndex = _.findIndex(rating, (o) => (o.resource_reference == chunk[j].resource_reference));
                const index = _.findIndex(allResources, (o) => (o.resource_reference == chunk[j].resource_reference));
                if (viewIndex !== -1) {
                    allResources[index].views = views[viewIndex].views;
                    allResources[index].et = views[viewIndex].et;
                    allResources[index].st = views[viewIndex].st;
                } else {
                    allResources[index].views = "0";
                    allResources[index].et = "0";
                    allResources[index].st = "0";
                }
                if (ratingIndex !== -1) {
                    allResources[index].sum_rating = rating[ratingIndex].sum_rating;
                    allResources[index].ct_rating = rating[ratingIndex].ct_rating;
                } else {
                    allResources[index].sum_rating = "0";
                    allResources[index].ct_rating = "0";
                }
            }
        }
        const groupByExpert = _.groupBy(allResources, "expert_id");
        const dataWithStats = [];
        // et, st, et_per_st and avg_rating calculation for each expert chunk - step 3
        for (const expertID in groupByExpert) {
            if ({}.hasOwnProperty.call(groupByExpert, expertID)) {
                const resources = groupByExpert[expertID];
                const groupByMasterChapter = _.groupBy(resources, "master_chapter");
                for (const masterChapter in groupByMasterChapter) {
                    if ({}.hasOwnProperty.call(groupByMasterChapter, masterChapter)) {
                        const resources1 = groupByMasterChapter[masterChapter];
                        const groupBySubject = _.groupBy(resources1, "subject");
                        for (const subject in groupBySubject) {
                            const resources2 = groupBySubject[subject];
                            const groupByClass = _.groupBy(resources2, "class");
                            for (const className in groupByClass) {
                                const resources3 = groupByClass[className];
                                const groupByLocale = _.groupBy(resources3, "locale");
                                for (const locale in groupByLocale) {
                                    const resources4 = groupByLocale[locale];
                                    const groupByCourseExam = _.groupBy(resources4, "course_exam");
                                    for (const courseExam in groupByCourseExam) {
                                        const resources5 = groupByCourseExam[courseExam];
                                        const groupByIsFree = _.groupBy(resources5, "is_free");
                                        for (const isFree in groupByIsFree) {
                                            const finalResources = groupByIsFree[isFree];
                                            let et = 0;
                                            let st = 0;
                                            let sumRating = 0;
                                            let ctRating = 0;
                                            for (let i = 0; i < finalResources.length; i++) {
                                                et += parseInt(finalResources[i].et);
                                                st += parseInt(finalResources[i].st);
                                                sumRating += parseInt(finalResources[i].sum_rating);
                                                ctRating += parseInt(finalResources[i].ct_rating);
                                            }
                                            let etPerSt = 0;
                                            if (st !== 0) {
                                                etPerSt = et / st;
                                            }
                                            let avgRating = 0;
                                            if (ctRating !== 0) {
                                                avgRating = sumRating / ctRating;
                                            }
                                            const data = {
                                                expert_id: expertID,
                                                master_chapter: masterChapter,
                                                subject,
                                                class: className,
                                                locale,
                                                course_exam: courseExam,
                                                is_free: isFree,
                                                et,
                                                st,
                                                et_per_st: etPerSt,
                                                avg_rating: avgRating,
                                            };
                                            dataWithStats.push(data);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        // ranking calculation for each masterchapter chunk - step 4 & 5
        const finalRanking = [];
        const groupByMasterChapter = _.groupBy(dataWithStats, "master_chapter");
        for (const masterChapter in groupByMasterChapter) {
            if ({}.hasOwnProperty.call(groupByMasterChapter, masterChapter)) {
                const resources1 = groupByMasterChapter[masterChapter];
                const groupBySubject = _.groupBy(resources1, "subject");
                for (const subject in groupBySubject) {
                    const resources2 = groupBySubject[subject];
                    const groupByClass = _.groupBy(resources2, "class");
                    for (const className in groupByClass) {
                        const resources3 = groupByClass[className];
                        const groupByLocale = _.groupBy(resources3, "locale");
                        for (const locale in groupByLocale) {
                            const resources4 = groupByLocale[locale];
                            const groupByCourseExam = _.groupBy(resources4, "course_exam");
                            for (const courseExam in groupByCourseExam) {
                                const resources5 = groupByCourseExam[courseExam];
                                const groupByIsFree = _.groupBy(resources5, "is_free");
                                for (const isFree in groupByIsFree) {
                                    const finalResources = groupByIsFree[isFree];
                                    const etSort = _.sortBy(finalResources, "et", "desc");
                                    const etPerStSort = _.sortBy(finalResources, "et_per_st", "desc");
                                    const avgRatingSort = _.sortBy(finalResources, "avg_rating", "desc");
                                    for (let i = 0; i < finalResources.length; i++) {
                                        const rankEt = _.findIndex(etSort, (o) => o.expert_id === finalResources[i].expert_id) + 1;
                                        const rankEtPerSt = _.findIndex(etPerStSort, (o) => o.expert_id === finalResources[i].expert_id) + 1;
                                        const rankAvgRating = _.findIndex(avgRatingSort, (o) => o.expert_id === finalResources[i].expert_id) + 1;
                                        finalResources[i].rank_et = rankEt;
                                        finalResources[i].rank_et_per_st = rankEtPerSt;
                                        finalResources[i].rank_avg_rating = rankAvgRating;
                                    }
                                    for (let i = 0; i < finalResources.length; i++) {
                                        finalResources[i].score = finalResources[i].rank_et + finalResources[i].rank_et_per_st + finalResources[i].rank_avg_rating;
                                    }
                                    const scoreSort = _.sortBy(finalResources, "score", "desc");
                                    finalRanking.push(scoreSort[0]);
                                }
                            }
                        }
                    }
                }
            }
        }

        // step 8 :- 40 - 60 split
        // const finalRankingSplit = [];
        // const groupByClass = _.groupBy(finalRanking, "class");
        // for (const className in groupByClass) {
        //     if ({}.hasOwnProperty.call(groupByClass, className)) {
        //         const resources = groupByClass[className];
        //         const groupByLocale = _.groupBy(resources, "locale");
        //         for (const locale in groupByLocale) {
        //             const resources1 = groupByLocale[locale];
        //             const groupByCourseExam = _.groupBy(resources1, "course_exam");
        //             for (const courseExam in groupByCourseExam) {
        //                 const resources2 = groupByCourseExam[courseExam];
        //                 const groupBySubject = _.groupBy(resources2, "subject");
        //                 for (const subject in groupBySubject) {
        //                     const resources3 = groupBySubject[subject];
        //                     const groupByMasterChapterSplit = _.groupBy(resources3, "master_chapter");
        //                     for (const masterChapter in groupByMasterChapterSplit) {
        //                         const resources4 = groupByMasterChapterSplit[masterChapter];
        //                         const paidResources = resources4.filter((o) => o.is_free == 1);
        //                         const freeResources = resources4.filter((o) => o.is_free == 0);
        //                         const lengthPaid = paidResources.length;
        //                         const lengthFree = freeResources.length;
        //                         const finalRankingTemp = [];
        //                         // if paid resources are less than 40% of total resources
        //                         if (lengthPaid / (lengthPaid + lengthFree) <= 0.4) {
        //                             finalRankingTemp.push(...resources4);
        //                         } else {
        //                             const splitFree = lengthFree;
        //                             let splitPaid = lengthPaid;
        //                             // make the split 40% paid and 60% free
        //                             if (splitFree > 0) {
        //                                 splitPaid = (splitFree / 0.6) * 0.4;
        //                             }
        //                             const splitPaidResources = _.take(paidResources, splitPaid);
        //                             const splitFreeResources = _.take(freeResources, splitFree);
        //                             finalRankingTemp.push(...splitPaidResources);
        //                             finalRankingTemp.push(...splitFreeResources);
        //                         }
        //                         finalRankingSplit.push(...finalRankingTemp);
        //                     }
        //                 }
        //             }
        //         }
        //     }
        // }

        const finalRankingSplit = finalRanking;
        // step 9 :- push resources of the ranking
        const finalResourcesAll = [];
        for (let i = 0; i < finalRankingSplit.length; i++) {
            let resources = allResources.filter((o) => o.master_chapter === finalRankingSplit[i].master_chapter && o.subject === finalRankingSplit[i].subject && o.class === finalRankingSplit[i].class && o.locale === finalRankingSplit[i].locale && o.course_exam === finalRankingSplit[i].course_exam && o.is_free === finalRankingSplit[i].is_free && o.expert_id === finalRankingSplit[i].expert_id);
            for (let j = 0; j < resources.length; j++) {
                resources[j].avg_rating = finalRankingSplit[i].avg_rating;
                resources[j].et_per_st = finalRankingSplit[i].et_per_st;
            }
            resources = resources.filter((o) => !isNaN(+o.liveclass_course_id) && !isNaN(+o.liveclass_course_detail_id) && !isNaN(+o.class) && !isNaN(+o.resource_reference) && !isNaN(+o.avg_rating) && !isNaN(+o.et_per_st) && !isNaN(+o.expert_id));
            finalResourcesAll.push(...resources);
        }
        const finalResourcesAllQidsArray = finalResourcesAll.map((o) => o.resource_reference);
        let getActiveEntries = [];
        const chunkSize = 1000;
        for (let i = 0; i < finalResourcesAllQidsArray.length; i += chunkSize) {
            console.log(`checking old entries for ${i} to ${i + chunkSize}`);
            const chunk = finalResourcesAllQidsArray.slice(i, i + chunkSize);
            const getActiveEntriesTemp = await getActiveTopFreeClasses(chunk);
            getActiveEntries.push(...getActiveEntriesTemp);
        }
        getActiveEntries = _.uniqBy(getActiveEntries, "question_id");

        // disable old entries
        await disableOldEntries();

        // insert new entries or update old entries
        const getActiveEntriesQids = getActiveEntries.map((o) => o.question_id.toString());
        const insertEntries = finalResourcesAll.filter((item) => !getActiveEntriesQids.includes(item.resource_reference));
        const updateEntries = finalResourcesAll.filter((item) => getActiveEntriesQids.includes(item.resource_reference));
        console.log(`Final Resources All :- ${finalResourcesAll.length}`);
        console.log(`Insert Entries :- ${insertEntries.length}`);
        console.log(`Update Entries :- ${updateEntries.length}`);
        const chunk2 = 100;
        for (let i = 0; i < insertEntries.length; i += chunk2) {
            console.log(`inserting resources - ${i} - ${i + chunk2}`);
            const resourceChunk = insertEntries.slice(i, i + chunk2);
            const workers = [];
            for (let j = 0; j < resourceChunk.length; j++) {
                const insertObj = {
                    id: null,
                    liveclass_course_id: parseInt(resourceChunk[j].liveclass_course_id),
                    course_exam: resourceChunk[j].course_exam,
                    locale: resourceChunk[j].locale,
                    detail_id: resourceChunk[j].liveclass_course_detail_id,
                    chapter_order: null,
                    master_chapter: resourceChunk[j].master_chapter,
                    chapter: resourceChunk[j].chapter,
                    class: parseInt(resourceChunk[j].class),
                    subject: resourceChunk[j].subject,
                    expert_name: resourceChunk[j].expert_name,
                    question_id: parseInt(resourceChunk[j].resource_reference),
                    average_rating: resourceChunk[j].avg_rating,
                    et_per_st: resourceChunk[j].et_per_st,
                    is_active: 1,
                    expert_id: parseInt(resourceChunk[j].expert_id),
                };
                workers.push(insertResource(insertObj));
            }
            await Promise.all(workers);
        }
        const chunk3 = 100;
        for (let i = 0; i < updateEntries.length; i += chunk3) {
            console.log(`updating resources - ${i} - ${i + chunk3}`);
            const resourceChunk = updateEntries.slice(i, i + chunk3);
            const workers2 = [];
            for (let j = 0; j < resourceChunk.length; j++) {
                const index = _.findIndex(getActiveEntries, (o) => (o.question_id == resourceChunk[j].resource_reference));
                let id;
                if (index > -1) {
                    id = getActiveEntries[index].id;
                    workers2.push(updateResource(resourceChunk[j], id));
                }
            }
            await Promise.all(workers2);
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
    cron: "30 20 * * 6",
};
