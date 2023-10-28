const appXHelper = require("./appx.helper");
const TestAddAssortment = require("./test-add-assortment");

function sqsTrigger(sqs, sqs_queue_url, data) {
    const params = {
        MessageBody: JSON.stringify(data),
        QueueUrl: sqs_queue_url,
    };
    sqs.sendMessage(params, (err, data) => {
        if (err) {
            console.error("sqstrigger :", err);
        } else {
            console.log(data);
        }
    });
}

async function createDnFromAppxSync(db, appxSyncData, resource_type, syncId, sqs) {
    console.info(syncId, "SYNC ID");
    /* Transform For Entries For LiveClass */
    const liveClassData = await appXHelper.tranformAppxToDN(appxSyncData, db);

    /*  Questions Meta */
    const questionsMeta = appXHelper.createQuestionsMetaFromLive(liveClassData, resource_type);

    /* Create Question */
    const questionInsert = await appXHelper.addQuestion(db, questionsMeta);

    const questionID = questionInsert.insertId;

    liveClassData.question_id = questionID;
    /* Update Qid in AppX */

    await appXHelper.updateAppxSync(db, { question_id: questionID }, syncId);

    console.log(syncId);
    /* Answer Meta */
    const answer = {};
    answer.expert_id = liveClassData.faculty_id;
    answer.question_id = questionID;
    answer.answer_video = appxSyncData.file_link;
    answer.youtube_id = questionsMeta.doubt;

    console.log(answer);
    /* Create Answer */
    const answerInsert = await appXHelper.addAnswer(db, answer);
    const answerID = answerInsert.insertId;

    /* Answer Video Resource Object */
    const answerVideoResource = {
        answer_id: answerID,
        resource: appxSyncData.file_link,
        resource_type: resource_type == 1 ? "BLOB" : "HLS",
        resource_order: 1,
        is_active: 1,
    };

    /* add Answer Video Resource */
    await appXHelper.addAnswerVideoResource(db, answerVideoResource);

    /* Trigger Transcoding */
    if (resource_type == 1) {
        sqsTrigger(sqs, "https://sqs.ap-south-1.amazonaws.com/942682721582/FERMI_TENCENT_LC", {
            questionId: questionID,
            url: [appxSyncData.file_link],
            entityType: "appx-exampur",
        });
    }

    /* Create LiveCourse Details */
    const courseDetailEntry = await appXHelper.insertUpcomingClassIntoCourseDetails(db, liveClassData);
    const detailID = courseDetailEntry.insertId;

    /* Insert Course Resource */
    await appXHelper.insertUpcomingClassIntoCourseResources(db, detailID, liveClassData, questionID, resource_type);

    /* Transform the Schema to New Class */

    await TestAddAssortment.main(db, true, detailID);

    await appXHelper.updateAppxSync(db, { is_processed: 1, old_detail_id: detailID }, syncId);

    return 1;
}
module.exports = { createDnFromAppxSync, sqsTrigger };
