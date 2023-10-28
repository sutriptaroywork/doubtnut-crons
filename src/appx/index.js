/* eslint-disable no-await-in-loop */
const bent = require("bent");
const bluebird = require("bluebird");
const Redis = require("ioredis");
const _ = require("lodash");
const { aws, mysql } = require("../../modules");
const appXHelper = require("./appx.helper");
const liveclassHelper = require("./liveclass.helper");
// const TestAddAssortment = require("./test-add-assortment");
const assortment_package_generation = require("./assortment_package_generation");

const { sqs } = aws;
const appxCourseMap = {
    54: "148", 172: "149", 201: "150", 209: "151", 234: "152", 235: "153", 241: "154", 1009: "155", 1016: "156", 1018: "157", 1021: "158", 1024: "159", 1026: "160", 1036: "161", 1037: "162", 1045: "163",
};
bluebird.promisifyAll(Redis);
let runPackage = false;

class DBWrapper {
    constructor(db) {
        this.client = db;
    }

    query(sql, ...params) {
        return this.client.query(sql, ...params).then((res) => res[0]);
    }
}
const db = new DBWrapper(mysql.writePool);

const appxApi = bent("https://exampurdb.com/api/get", "GET", "json", 200, {
    "Client-Service": "ExampurApp",
    "Auth-Key": "exampurapi",
});
const appxEndApi = bent("https://api.doubtnut.com/v2/liveclass", "POST", "json", 200, {
    "x-auth-token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6NDQxNDUxMCwiaWF0IjoxNTc5NTAzODc0LCJleHAiOjE2NDI1NzU4NzR9.q8nf7dOc87qjnTweblWZh43kTSjSZsAXE_pShwq8Fts",
});
const appxDeletCacheApi = bent("https://api.doubtnut.com/v2/liveclass", "GET", "json", 200, {
    "x-auth-token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6NDQxNDUxMCwiaWF0IjoxNTc5NTAzODc0LCJleHAiOjE2NDI1NzU4NzR9.q8nf7dOc87qjnTweblWZh43kTSjSZsAXE_pShwq8Fts",
});
const fermiTranscodeApi = bent("http://gateway.doubtnut.internal", "PUT", "json", 200, {
    "x-auth-token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6NDQxNDUxMCwiaWF0IjoxNTc5NTAzODc0LCJleHAiOjE2NDI1NzU4NzR9.q8nf7dOc87qjnTweblWZh43kTSjSZsAXE_pShwq8Fts",
});

async function updateStatusToLive(liveClasses, appXCourse) {
    //* Get Live Records From Db
    const alreadyLiveRecords = await appXHelper.getListCurrentLiveClassByAppxCourseId(db, appXCourse);
    const groupedLive = _.groupBy(alreadyLiveRecords, "id");
    for (const liveClass of liveClasses) {
        liveClass.appx_course_id = appXCourse;
        liveClass.liveclass_course_id = appxCourseMap[appXCourse];
        delete groupedLive[liveClass.id];
        /* Get Appx Sync Data To check If Class Status Already Updated Or Not */
        let appxSyncData = await appXHelper.getAppxSyncDataById(db, liveClass.id);
        /* Skip If Status is Already Updated */
        console.log(appxSyncData);
        if (!appxSyncData.length) {
            /* sync with AppXSync */
            const insertAppXsync = await appXHelper.insertIntoAppXSync(db, liveClass);
            console.log(insertAppXsync);
            console.log(insertAppXsync.insertId);
            if (insertAppXsync.affectedRows == 0) continue;
            /* Create Recorded Resource In LiveClass Schema */
            await liveclassHelper.createDnFromAppxSync(db, liveClass, "4", insertAppXsync.insertId, sqs);
            runPackage = true;

            appxSyncData = await appXHelper.getAppxSyncDataById(db, liveClass.id);
        } else if (appxSyncData[0].status == liveClass.status) {
            console.log("synced class");

            continue;
        }

        /* Get Course Resource By Resource Reference (QID) */
        const courseResourceData = await appXHelper.getCourseResourceByResourceReference(db, appxSyncData[0].question_id);

        /* Get Question With Answer Data */
        const questionWithAnswer = await appXHelper.getByQuestionId(db, appxSyncData[0].question_id);
        /* Update Answer Video Resource */
        await appXHelper.updateAnswerVideoResource(db, { resource: liveClass.file_link }, questionWithAnswer[0].answer_id);
        /* Update Stream Status To Active */
        await appXHelper.updateStreamStartTime(db, courseResourceData[0].id);

        fermiTranscodeApi("/api/fermi/rtmp/start", {
            questionId: appxSyncData[0].question_id,
            streamUrl: liveClass.file_link,
            delegate: true,
            stopDelay: 120,
        });
        await appXHelper.updateAppxSync(db, { status: liveClass.status }, appxSyncData[0].sync_id);

        await appxDeletCacheApi(`/deletecache?resource_id=${courseResourceData[0].id}`);

        /* Update status of appXSync */
    }

    /* End Missing Live Classes */
    console.log(groupedLive);
    for (const groupedLiveKey in groupedLive) {
        if (groupedLive[groupedLiveKey][0] && groupedLive[groupedLiveKey][0].question_id) {
            const questionId = groupedLive[groupedLiveKey][0].question_id;
            fermiTranscodeApi("/api/fermi/rtmp/stop", {
                questionId,
            });
        }
    }

    return 1;
}
async function createUpcomingClasses(upcoming, appXCourse) {
    for (const upcomingClass of upcoming) {
        upcomingClass.appx_course_id = appXCourse;

        upcomingClass.liveclass_course_id = appxCourseMap[appXCourse];
        if (upcomingClass.material_type !== "VIDEO") {
            continue;
        }
        const appxSyncData = await appXHelper.getAppxSyncDataById(db, upcomingClass.id);
        console.log(appxSyncData);
        /* Check If Record Exist and Status  */
        if (!appxSyncData.length) {
            /* sync with AppXSync */
            const insertAppXsync = await appXHelper.insertIntoAppXSync(db, upcomingClass);

            if (insertAppXsync.affectedRows == 0) { continue; }
            /* Create Recorded Resource In LiveClass Schema */
            await liveclassHelper.createDnFromAppxSync(db, upcomingClass, "4", insertAppXsync.insertId, sqs);
            runPackage = true;
        }
    }
    return 1;
}

async function syncRecordedClasses(recorded, appXCourse) {
    for (let i = (recorded.length - 1); i >= 0; i--) {
        const recordedResource = recorded[i];
        recordedResource.appx_course_id = appXCourse;
        recordedResource.liveclass_course_id = appxCourseMap[appXCourse];
        console.log(recordedResource.material_type);
        if (recordedResource.material_type !== "VIDEO") {
            console.log("not video");
            continue;
        }
        const appxSyncData = await appXHelper.getAppxSyncDataById(db, recordedResource.id);
        /* Check If Record Exist and Status  */
        if (!appxSyncData.length) {
            /* sync with AppXSync */
            console.log("new class");
            const insertAppXsync = await appXHelper.insertIntoAppXSync(db, recordedResource);
            if (insertAppXsync.affectedRows == 0) continue;
            /* Create Recorded Resource In LiveClass Schema */

            await liveclassHelper.createDnFromAppxSync(db, recordedResource, "1", insertAppXsync.insertId, sqs);
            runPackage = true;
        } else if (appxSyncData[0].status != recordedResource.status) {
            console.log("status change");
            /* custom end call */
            const courseResourceData = await appXHelper.getCourseResourceByResourceReference(db, appxSyncData[0].question_id);
            liveclassHelper.sqsTrigger(sqs, "https://sqs.ap-south-1.amazonaws.com/942682721582/FERMI_TENCENT_LC", {
                questionId: appxSyncData[0].question_id,
                url: [recordedResource.file_link],
                entityType: "appx-exampur",
            });

            await appxEndApi(`/endappx?resource_id=${courseResourceData[0].id}`, { video_url: recordedResource.file_link });
            fermiTranscodeApi("/api/fermi/rtmp/stop", {
                questionId: appxSyncData[0].question_id,
            });
            await appXHelper.updateAppxSync(db, { status: recordedResource.status }, appxSyncData[0].sync_id);
        }
    }
    return 1;
}
async function start(job) {
    try {
        const appXCourses = [54, 172, 201, 209, 234, 235, 241, 1009, 1016, 1018, 1021, 1024, 1026, 1036, 1037, 1045];
        for (let i = 0; i < appXCourses.length; i++) {
            const appXCourse = appXCourses[i];
            const appxData = await appxApi(`/doubtnut_live_upcoming_recorded_course_class?courseid=${appXCourse}`);
            if (appxData.status !== 200) return "appX call fails";
            /* First Updating Status Of LiveClasses */
            const { live } = appxData.data;
            const { upcoming } = appxData.data;
            const { recorded } = appxData.data;
            // promise.push(updateStatusToLive(live, appXCourse));
            // promise.push(createUpcomingClasses(upcoming, appXCourse))
            // promise.push(syncRecordedClasses(recorded, appXCourse))
            // console.log(recorded);
            await Promise.all([
                updateStatusToLive(live, appXCourse), createUpcomingClasses(upcoming, appXCourse), syncRecordedClasses(recorded, appXCourse),
            ]);
        }

        if (runPackage) {
            await assortment_package_generation.main(db);
            runPackage = false;
        }

        await job.progress(100);
        return { status: 1 };
    } catch (err) {
        console.log(err);
        return { err };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "*/1 * * * *",
    concurrency: 1, // optional
    removeOnComplete: 30, // optional
    removeOnFail: 30, // optional
    disabled: true,
};
