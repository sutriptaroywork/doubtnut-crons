/* eslint-disable guard-for-in */
const _ = require("lodash");
const {
    redis, aws, config, redshift,
} = require("../../modules");

async function getAvailableClassesAndSubjectsForQuiz() {
    const sql = `select
	DISTINCT 
    a.class,
    a.subject,
    b.chapter_alias_url as chapter_alias_url,
    d.package_language
from
    classzoo1.questions a
    left join classzoo1.chapter_alias_all_lang b on a.class = b.class
    and a.chapter = b.chapter
    left join classzoo1.text_solutions c on a.question_id = c.question_id
    left join classzoo1.studentid_package_mapping_new d on a.student_id = d.student_id
where
    a.student_id < 100
    and a.student_id not in (
        -248,
        -228,
        -223,
        -140,
        -64,
        -63,
        -41,
        -26,
        83,
        85,
        86,
        96
    )
    and is_text_answered = 1
    and a.chapter is not null
    and b.id is not null
    and c.id is not null
    and c.opt_1 is not null
    and c.opt_1 <> ''
    and c.opt_2 is not null
    and c.opt_2 <> ''
    and c.opt_3 is not null
    and c.opt_3 <> ''
    and c.opt_4 is not null
    and c.opt_4 <> ''
    and c.answer is not null
    and c.solutions <> 'N/A'
    and c.solutions <> ''
    and c.solutions is not null
group by
    a.class,
    a.subject,
    b.chapter_alias_url,
    d.package_language`;
    return redshift.query(sql);
}

async function getWhatsappChapterAndQuestions(studentClass, subject, chapter) {
    const sql = `select
            d.chapter_alias as chapter,
            c.package_language,
            b.question_id,
            d.chapter_alias_url
        from
            classzoo1.questions as a
            left join classzoo1.text_solutions b on a.question_id = b.question_id
            left join classzoo1.studentid_package_mapping_new c on a.student_id = c.student_id
            left join classzoo1.chapter_alias_all_lang d on a.class = d.class
            and a.chapter = d.chapter
        where
            a.is_text_answered = 1
            and a.class = '${studentClass}'
            and a.subject = '${subject}'
            and d.chapter_alias_url='${chapter}'
            and a.student_id < 100
            and a.student_id not in (
                -248,
                -228,
                -223,
                -140,
                -64,
                -63,
                -41,
                -26,
                83,
                85,
                86,
                96
            )
            and a.chapter is not null
            and b.id is not null
            and b.opt_1 is not null
            and b.opt_1 <> ''
            and b.answer is not null
            and d.chapter_alias is not null
            and b.opt_2 is not null
            and b.opt_2 <> ''
            and b.opt_3 is not null
            and b.opt_3 <> ''
            and b.opt_4 is not null
            and b.opt_4 <> ''
            and b.solutions <> 'N/A'
            and b.solutions <> ''
            and b.solutions is not null
            limit 100000
        `;
    return redshift.query(sql);
}
async function questionIdExistsInS3(locale, qid) {
    if (!locale || !qid) {
        return false;
    }
    try {
        await aws.s3.headObject({
            Bucket: config.staticBucket,
            Key: `question-text/${locale}/${qid}.png`,
        }).promise();
        return true;
    } catch (e) {
        return false;
    }
}
async function start(job) {
    try {
        job.progress(0);
        console.log("here");
        let result = await getAvailableClassesAndSubjectsForQuiz();
        console.log("here");
        const excludedSubjectForClass11and12 = ["ECONOMICS", "ACCOUNTS", "GENERAL KNOWLEDGE", "GENERAL KNOWLEDGE AND APTITUDE"];
        result = result.filter((item) => {
            if (+item.class === 11 || +item.class === 12) {
                return !excludedSubjectForClass11and12.includes(item.subject);
            }
            return true;
        });
        result.forEach((item) => {
            item.class = item.class.trim();
            item.subject = item.subject.trim();
            item.package_language = item.package_language.trim();
        });
        const classesMapping = _.groupBy(result, "class");
        const availableClasses = Object.keys(classesMapping);
        let i = 0;
        const allClasses = new Set();
        const allLang = new Set();
        const allSub = new Set();
        const availableLanguageMappingForSubjects = {};
        const availableClassesMappingForSubjects = {};
        const availableClassesForLanguage = {};
        const availableSubjectsForLanguage = {};

        for (const classKey in classesMapping) {
            if (classesMapping[classKey]) {
                const availableSubjectsForLanguageClass = {};
                const availableLanguageForClass = new Set();
                const subjectsMapping = _.groupBy(classesMapping[classKey], "subject");
                const finalAvailableSubjects = new Set();
                for (const subjectKey in subjectsMapping) {
                    if (subjectsMapping[subjectKey]) {
                        // eslint-disable-next-line no-await-in-loop
                        const chapterMapping = _.groupBy(subjectsMapping[subjectKey], "chapter_alias_url");
                        const availableLanguagesForSubjectClass = new Set();
                        const availableChaptersForLanguage = {};
                        for (const chapter_alias_url in chapterMapping) {
                            if (chapterMapping[chapter_alias_url]) {
                                const chapterResult = await getWhatsappChapterAndQuestions(classKey, subjectKey, chapter_alias_url);
                                console.log(classKey, subjectKey, chapter_alias_url);
                                const filteredResult = [];
                                chapterResult.forEach((item) => {
                                    item.package_language = item.package_language.trim();
                                    item.chapter = item.chapter.toUpperCase().trim();
                                    item.chapter_alias_url = item.chapter_alias_url ? item.chapter_alias_url.toUpperCase().trim() : item.chapter.toUpperCase().trim();
                                });
                                const questionsExistsPromises = [];
                                for (let j = 0; j < chapterResult.length; j++) {
                                    // eslint-disable-next-line no-await-in-loop
                                    questionsExistsPromises.push(questionIdExistsInS3(chapterResult[j].package_language, chapterResult[j].question_id));
                                }
                                // eslint-disable-next-line no-await-in-loop
                                const questionsExists = await Promise.all(questionsExistsPromises);
                                for (let j = 0; j < chapterResult.length; j++) {
                                    if (questionsExists[j]) {
                                        filteredResult.push(chapterResult[j]);
                                    }
                                }

                                const languageMapping = _.groupBy(filteredResult, "package_language");
                                const availableLanguages = Object.keys(languageMapping);
                                availableLanguages.forEach((item) => availableLanguagesForSubjectClass.add(item));
                                if (filteredResult.length) {
                                    allClasses.add(classKey);
                                    allSub.add(subjectKey);
                                    if (availableLanguageMappingForSubjects[subjectKey]) {
                                        availableLanguages.forEach((lang) => availableLanguageMappingForSubjects[subjectKey].add(lang));
                                    } else {
                                        availableLanguageMappingForSubjects[subjectKey] = new Set(availableLanguages);
                                    }
                                    if (availableClassesMappingForSubjects[subjectKey]) {
                                        availableClassesMappingForSubjects[subjectKey].add(classKey);
                                    } else {
                                        availableClassesMappingForSubjects[subjectKey] = new Set([classKey]);
                                    }
                                    for (const languages of availableLanguages) {
                                        allLang.add(languages);
                                        availableLanguageForClass.add(languages);
                                        if (availableClassesForLanguage[languages]) {
                                            availableClassesForLanguage[languages].add(classKey);
                                        } else {
                                            availableClassesForLanguage[languages] = new Set([classKey]);
                                        }
                                    }
                                }
                                for (const languageKey in languageMapping) {
                                    if (languageMapping[languageKey]) {
                                        const questions = _.groupBy(languageMapping[languageKey], "question_id");
                                        const questionIdsArray = Object.keys(questions);
                                        if (questionIdsArray.length >= 10) {
                                            if (availableChaptersForLanguage[languageKey]) {
                                                availableChaptersForLanguage[languageKey].push({
                                                    chapter: languageMapping[languageKey][0].chapter,
                                                    chapter_alias_url: languageMapping[languageKey][0].chapter_alias_url,

                                                });
                                            } else {
                                                availableChaptersForLanguage[languageKey] = [{
                                                    chapter: languageMapping[languageKey][0].chapter,
                                                    chapter_alias_url: languageMapping[languageKey][0].chapter_alias_url,
                                                }];
                                            }
                                            finalAvailableSubjects.add(subjectKey);
                                            if (availableSubjectsForLanguage[languageKey]) {
                                                availableSubjectsForLanguage[languageKey].add(subjectKey);
                                            } else {
                                                availableSubjectsForLanguage[languageKey] = new Set([subjectKey]);
                                            }
                                            if (availableSubjectsForLanguageClass[languageKey]) {
                                                availableSubjectsForLanguageClass[languageKey].add(subjectKey);
                                            } else {
                                                availableSubjectsForLanguageClass[languageKey] = new Set([subjectKey]);
                                            }

                                            redis.setAsync(`QUIZ:WA:QUESTIONS:${classKey}:${subjectKey}:${languageKey}:${chapter_alias_url.toUpperCase()}`, JSON.stringify(questionIdsArray.slice(0, 200)), "Ex", 60 * 60 * 24 * 7);
                                        }
                                    }
                                }
                            }
                        }
                        redis.setAsync(`QUIZ:WA:CLASS:LANGUAGES:${classKey}:${subjectKey}`, JSON.stringify([...availableLanguagesForSubjectClass]), "Ex", 60 * 60 * 24 * 7);

                        for (const languageKey in availableChaptersForLanguage) {
                            const chapters = availableChaptersForLanguage[languageKey];
                            redis.setAsync(`QUIZ:WA:CLASS:CHAPTERS:${classKey}:${subjectKey}:${languageKey}`, JSON.stringify(_.uniqBy(chapters, "chapter_alias_url")), "Ex", 60 * 60 * 24 * 7);
                            console.log(`QUIZ:WA:CLASS:CHAPTERS:${classKey}:${subjectKey}:${languageKey}`,JSON.stringify(_.uniqBy(chapters, "chapter_alias_url")));
                        }
                    }
                }
                console.log("availableSubjectsForLanguageClass", availableSubjectsForLanguageClass);
                redis.setAsync(`QUIZ:WA:CLASS:${classKey}:LANGUAGES`, JSON.stringify([...availableLanguageForClass]), "Ex", 60 * 60 * 24 * 7);
                for (const languageKey in availableSubjectsForLanguageClass) {
                    console.log(`${classKey} and ${languageKey}`, JSON.stringify([...availableSubjectsForLanguageClass[languageKey]]));
                    console.log(`QUIZ:WA:CLASS:${classKey}:LANG:${languageKey}:SUBJECTS`);
                    redis.setAsync(`QUIZ:WA:CLASS:${classKey}:LANG:${languageKey}:SUBJECTS`, JSON.stringify([...availableSubjectsForLanguageClass[languageKey]]), "Ex", 60 * 60 * 24 * 7);
                }
                redis.setAsync(`QUIZ:WA:CLASS:SUBJECTS:${classKey}`, JSON.stringify([...finalAvailableSubjects]), "Ex", 60 * 60 * 24 * 7);

                job.progress(parseInt(((i + 1) / availableClasses.length) * 100));
                i++;
            }
        }
        redis.setAsync("QUIZ:WA:CLASSES", JSON.stringify([...allClasses]), "Ex", 60 * 60 * 24 * 7);
        redis.setAsync("QUIZ:WA:LANG", JSON.stringify([...allLang]), "Ex", 60 * 60 * 24 * 7);
        redis.setAsync("QUIZ:WA:SUB", JSON.stringify([...allSub]), "Ex", 60 * 60 * 24 * 7);
        for (const subjectKey in availableLanguageMappingForSubjects) {
            if (Object.prototype.hasOwnProperty.call(availableLanguageMappingForSubjects, subjectKey)) {
                console.log(`QUIZ:WA:SUB:${subjectKey}:LANGUAGES`, [...availableLanguageMappingForSubjects[subjectKey]]);
                redis.setAsync(`QUIZ:WA:SUB:${subjectKey}:LANGUAGES`, JSON.stringify([...availableLanguageMappingForSubjects[subjectKey]]), "Ex", 60 * 60 * 24 * 7);
            }
        }
        for (const subjectKey in availableClassesMappingForSubjects) {
            if (Object.prototype.hasOwnProperty.call(availableClassesMappingForSubjects, subjectKey)) {
                redis.setAsync(`QUIZ:WA:SUB:${subjectKey}:CLASSES`, JSON.stringify([...availableClassesMappingForSubjects[subjectKey]]), "Ex", 60 * 60 * 24 * 7);
            }
        }
        for (const languageKey in availableSubjectsForLanguage) {
            if (Object.prototype.hasOwnProperty.call(availableSubjectsForLanguage, languageKey)) {
                redis.setAsync(`QUIZ:WA:LANG:${languageKey}:SUBJECTS`, JSON.stringify([...availableSubjectsForLanguage[languageKey]]), "Ex", 60 * 60 * 24 * 7);
            }
        }
        for (const languageKey in availableClassesForLanguage) {
            if (Object.prototype.hasOwnProperty.call(availableClassesForLanguage, languageKey)) {
                redis.setAsync(`QUIZ:WA:LANG:${languageKey}:CLASSES`, JSON.stringify([...availableClassesForLanguage[languageKey]]), "Ex", 60 * 60 * 24 * 7);
            }
        }
        job.progress(100);
        console.log("Ended");
    } catch (err) {
        console.log(err);
        return { err };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 5 */3 * *", // * Every 3 days at 5 AM
};
