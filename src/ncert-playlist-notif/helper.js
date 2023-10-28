const { mysql } = require("../../modules");

function getPlaylistCreationParams(data) {
    const param = {};
    param.name = data.playlist_name;
    param.is_first = 0;
    param.is_last = 1;
    param.is_admin_created = 0;
    param.parent = 0;
    param.resource_path = 'select b.*,a.question_id,case when f.xxlanxx is null then a.ocr_text else f.xxlanxx end as ocr_text,a.doubt,a.question, case when b.chapter is null then a.chapter else b.chapter end as chapter,case when x.xxlanxx is null then b.chapter else x.xxlanxx end as chapter,case when y.xxlanxx is null then b.subtopic else y.xxlanxx end as subtopic, case when b.class is null then a.class else b.class end as class,e.packages from (Select question_id from playlist_questions_mapping where playlist_id=? and is_active=1) as t1 left join  (SELECT question_id,ocr_text,doubt,question,chapter,class from questions) as a on t1.question_id = a.question_id left join (select * from questions_meta where is_skipped = 0) as b on a.question_id = b.question_id left join (select GROUP_CONCAT(packages) as packages,question_id from question_package_mapping group by question_id) as e on a.question_id = e.question_id left join (select question_id,xxlanxx from questions_localized) as f on a.question_id=f.question_id left join (select chapter,min(xxlanxx) as xxlanxx from localized_ncert_chapter group by chapter) as x on a.chapter=x.chapter left join (select subtopic,min(xxlanxx) as xxlanxx from localized_subtopic group by subtopic) as y on b.subtopic=y.subtopic order by a.doubt ASC';
    param.resource_type = 'playlist';
    param.resource_description = 'playlist';
    param.student_class = data.student_class;
    param.student_course = data.student_course;
    param.playlist_order = 0;
    param.student_id = data.student_id;
    param.is_active = 1;
    return param;
}
function idetifierGenerator(splittedDoubt, splitter) {
    let identifier = '';
    for (let i = 0; i < splittedDoubt.length; i++) {
        if (i < (splittedDoubt.length - splitter)) {
            identifier = `${identifier + splittedDoubt[i]}_`;
        }
    }
    if (identifier == '') {
        return false;
    }
    return identifier;
}
async function getSimilarQuestionBelow100(identifier, doubt, student_class, student_id, limit) {
    const sql = 'SELECT *  FROM questions USE INDEX(doubt) WHERE doubt LIKE ?  and doubt > ? and is_answered=1  and student_id = ? AND class = ? ORDER by doubt ASC limit ?';
    return mysql.pool.query(sql, [`${identifier}%`, `${doubt}`, student_id, student_class, limit]).then((res) => res[0]);
}
async function getQuestionListBelow100(doubt, splittedDoubt, splitter, student_class, student_id, limit) {
    // Do async job
    try {
        const identifier = idetifierGenerator(splittedDoubt, splitter);
        const questionListData = await getSimilarQuestionBelow100(identifier, doubt, student_class, student_id, limit);
        return questionListData;
    } catch (e) {
        console.log(e);
        throw Error(e);
    }
}

async function rescursiveList(questionData, splitter, limit, questionList) {
    // Do async job
    try {
        const { doubt } = questionData;
        const student_class = questionData.class;
        const { student_id } = questionData;
        let leftQuestion; let questionList2;
        const splittedDoubt = doubt.split('_');
        if (questionList.length === 10 || splittedDoubt.length == splitter) {
            return questionList;
        }
        questionList2 = await getQuestionListBelow100(doubt, splittedDoubt, splitter, student_class, student_id, limit);
        if (questionList2.length === 0) {
            // console.log("1")
            if (splittedDoubt.length > splitter) {
                splitter += 1;
            }
            questionList2 = await rescursiveList(questionData, splitter, limit, questionList2);
            // console.log("zero___")
            // console.log(questionList2.length)
            return questionList2;
        }
        if (questionList2.length < 10) {
            leftQuestion = questionList;
            limit = 10 - questionList.length;

            // console.log("below 10")
            // console.log(leftQuestion.length)
            if (splittedDoubt.length > splitter) {
                splitter += 1;
            }
            questionList2 = await rescursiveList(questionData, splitter, limit, questionList);
            // concatenate both list
            // console.log(questionList2.length)
            questionList2 = [...leftQuestion, ...questionList2];
            // console.log(questionList2)
            return questionList2;
        }

        // console.log("3")
        // console.log("questionList2.length")
        // console.log(questionList2.length)
        return questionList2;
    } catch (e) {
        console.log(e);
        throw Error(e);
    }
}

module.exports = {
    getPlaylistCreationParams,
    rescursiveList,
};
