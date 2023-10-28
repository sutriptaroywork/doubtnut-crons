const axios = require("axios");
const moment = require("moment");
const _ = require("lodash");
const config = require("../../modules/config");

const maxDocPerStudentToRetain = 5;
const esHost = config.elasticsearch.host4;
const esIndexName = "user-questions";

async function axiosCall(method, url, data) {
    return axios({
        method,
        url,
        headers: { "Content-Type": "application/json" },
        data,
    });
}

async function getStudentBucketsWithExceddingDocs(maxDocs) {
    const url = `${esHost}${esIndexName}/_search`;
    const data = {
        size: 0,
        aggs: {
            student_id_wise: {
                terms: {
                    field: "student_id",
                    size: 5000,
                    min_doc_count: maxDocs,
                },
            },
        },
    };
    const response = await axiosCall("GET", url, data).then((res) => _.get(res, "data.aggregations.genres.buckets", []));
    return response;
}

async function getTimestampOfExceedingCountDoc(studentId) {
    const url = `${esHost}${esIndexName}/_search`;
    const q = {
        query: {
            match: {
                student_id: studentId,
            },
        },
        sort: [
            {
                timestamp: {
                    order: "desc",
                },
            },
        ],
    };
    const timestampOfExceedingCountDoc = await axiosCall("get", url, q).then((res) => _.get(res, `data.hits.hits[${maxDocPerStudentToRetain - 1}]._source.timestamp`, null));
    return timestampOfExceedingCountDoc;
}

async function deleteUnwantedLogs(studentId) {
    try {
        const timestampOfExceedingCountDoc = await getTimestampOfExceedingCountDoc(studentId);
        if (!_.isNull(timestampOfExceedingCountDoc)) {
            const url = `${esHost}${esIndexName}/_delete_by_query`;
            const q = {
                query: {
                    bool: {
                        must: [
                            {
                                range: {
                                    timestamp: {
                                        lt: timestampOfExceedingCountDoc,
                                    },
                                },
                            },
                            {
                                match: {
                                    student_id: studentId,
                                },
                            },
                        ],
                    },
                },
            };
            console.log(q);
            await axiosCall("POST", url, q).then((res) => console.log(res));
        }
        return true;
    } catch (e) {
        console.log(e);
    }
}

async function evictDocumentsByStudentsDocPolicy() {
    const studentIdsData = await getStudentBucketsWithExceddingDocs(maxDocPerStudentToRetain);
    for (let i = 0; i < studentIdsData.length; i++) {
        console.log(studentIdsData[i]);
        if (studentIdsData[i].doc_count > maxDocPerStudentToRetain) {
            await deleteUnwantedLogs(studentIdsData[i].key);
        }
    }
    return 0;
}

async function evictDocumentsAWeekOlder() {
    try {
        const timeBeforeOneWeek = moment().subtract(7, "day");
        const timestampBeforeOneWeek = moment(timeBeforeOneWeek).format("X");
        const url = `${esHost}${esIndexName}/_delete_by_query`;
        const data = {
            query: {
                range: {
                    timestamp: {
                        lte: timestampBeforeOneWeek,
                    },
                },
            },
        };
        const response = await axiosCall("POST", url, data);
        return response;
    } catch (e) {
        console.log(e);
    }
}

async function start(job) {
    await evictDocumentsAWeekOlder();
    job.progress(50);
    await evictDocumentsByStudentsDocPolicy();
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "*/15 * * * *", // every 15 minutes
};
