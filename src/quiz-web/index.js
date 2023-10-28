const _ = require("lodash");
const { redis, redshift } = require("../../modules");

async function getAvailableClassesAndSubjectsForQuiz() {
    const sql = "select a.class, a.subject, d.package_language from classzoo1.questions a left join classzoo1.chapter_alias_all_lang b on a.class = b.class and a.chapter = b.chapter left join classzoo1.text_solutions c on a.question_id = c.question_id left join classzoo1.studentid_package_mapping_new d on a.student_id = d.student_id where a.student_id < 100 and a.student_id not in (-248,-228,-223,-140,-64,-63,-41,-26,83,85,86,96) and (a.is_answered = 1 or a.is_text_answered = 1) and a.chapter is not null and b.id is not null and c.id is not null and c.opt_1 <> '' and c.answer is not null group by a.class, a.subject, d.package_language";
    return redshift.query(sql);
}

async function getChapterAndQuestions(studentClass, subject) {
    const sql = `select d.chapter_alias as chapter, c.package_language, b.question_id, d.chapter_alias_url from classzoo1.questions as a left join classzoo1.text_solutions b on a.question_id = b.question_id left join classzoo1.studentid_package_mapping_new c on a.student_id = c.student_id left join classzoo1.chapter_alias_all_lang d on a.class=d.class and a.chapter=d.chapter where (a.is_answered = 1 or a.is_text_answered = 1) and a.class='${studentClass}' and a.subject='${subject}' and a.student_id < 100 and a.student_id not in (-248,-228,-223,-140,-64,-63,-41,-26,83,85,86,96) and a.chapter is not null and b.id is not null and b.opt_1 is not null and b.opt_1 <> '' and b.answer is not null and d.chapter_alias is not null group by d.chapter_alias, c.package_language, b.question_id, b.created_at, d.chapter_alias_url order by b.created_at`;
    return redshift.query(sql);
}

async function start(job) {
    try {
        job.progress(0);
        let result = await getAvailableClassesAndSubjectsForQuiz();
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
        });
        const classesMapping = _.groupBy(result, "class");
        const availableLanguageMapping = _.groupBy(result, "package_language");
        const availableSubjectMapping = _.groupBy(result, "subject");
        const availableClasses = Object.keys(classesMapping);
        const availableLanguageMappingArr = Object.keys(availableLanguageMapping);
        const availableSubjectMappingArr = Object.keys(availableSubjectMapping);
        let i = 0;
        redis.setAsync("QUIZ:WEB:CLASSES", JSON.stringify(availableClasses), "Ex", 60 * 60 * 24 * 7);
        redis.setAsync("QUIZ:WEB:LANG", JSON.stringify(availableLanguageMappingArr), "Ex", 60 * 60 * 24 * 7);
        redis.setAsync("QUIZ:WEB:SUB", JSON.stringify(availableSubjectMappingArr), "Ex", 60 * 60 * 24 * 7);
        console.log("Available classes", availableClasses);
        for (const languageKey in availableLanguageMapping) {
            if (availableLanguageMapping[languageKey]) {
                const availableClassesInLang = _.groupBy(availableLanguageMapping[languageKey], "class");
                const availableSubjectsInLang = _.groupBy(availableLanguageMapping[languageKey], "subject");
                const availableClassesInLangArr = Object.keys(availableClassesInLang);
                const availableSubjectsInLangArr = Object.keys(availableSubjectsInLang);
                redis.setAsync(`QUIZ:WEB:LANG:${languageKey}:CLASSES`, JSON.stringify(availableClassesInLangArr), "Ex", 60 * 60 * 24 * 7);
                redis.setAsync(`QUIZ:WEB:LANG:${languageKey}:SUBJECTS`, JSON.stringify(availableSubjectsInLangArr), "Ex", 60 * 60 * 24 * 7);
            }
        }
        for (const subjectKey in availableSubjectMapping) {
            if (availableSubjectMapping[subjectKey]) {
                const availableClassesInSubject = _.groupBy(availableSubjectMapping[subjectKey], "class");
                const availableLangInSubject = _.groupBy(availableSubjectMapping[subjectKey], "package_language");
                const availableClassesInSubjectArr = Object.keys(availableClassesInSubject);
                const availableLangInSubjectArr = Object.keys(availableLangInSubject);
                redis.setAsync(`QUIZ:WEB:SUB:${subjectKey}:CLASSES`, JSON.stringify(availableClassesInSubjectArr), "Ex", 60 * 60 * 24 * 7);
                redis.setAsync(`QUIZ:WEB:SUB:${subjectKey}:LANGUAGES`, JSON.stringify(availableLangInSubjectArr), "Ex", 60 * 60 * 24 * 7);
            }
        }
        for (const classKey in classesMapping) {
            if (classesMapping[classKey]) {
                const subjectsMapping = _.groupBy(classesMapping[classKey], "subject");
                const availableSubjects = Object.keys(subjectsMapping);
                redis.setAsync(`QUIZ:WEB:CLASS:SUBJECTS:${classKey}`, JSON.stringify(availableSubjects), "Ex", 60 * 60 * 24 * 7);
                console.log(`Available subjects for ${classKey} are ${availableSubjects}`);
                for (const subjectKey in subjectsMapping) {
                    if (subjectsMapping[subjectKey]) {
                        const chapterResult = await getChapterAndQuestions(classKey, subjectKey);
                        chapterResult.forEach((item) => {
                            item.package_language = item.package_language.trim();
                        });
                        const languageMapping = _.groupBy(chapterResult, "package_language");
                        const availableLanguages = Object.keys(languageMapping);
                        redis.setAsync(`QUIZ:WEB:CLASS:LANGUAGES:${classKey}:${subjectKey}`, JSON.stringify(availableLanguages), "Ex", 60 * 60 * 24 * 7);
                        console.log(`Available languages for ${subjectKey} are ${availableLanguages}`);
                        for (const languageKey in languageMapping) {
                            if (languageMapping[languageKey]) {
                                languageMapping[languageKey].forEach((item) => {
                                    item.chapter = item.chapter.toUpperCase().trim();
                                    item.chapter_alias_url = item.chapter_alias_url ? item.chapter_alias_url.toUpperCase().trim() : item.chapter.toUpperCase().trim();
                                });
                                const chaptersMapping = _.groupBy(languageMapping[languageKey], "chapter_alias_url");
                                const availableChapters = languageMapping[languageKey]
                                    .reduce((acc, item) => {
                                        const index = acc.findIndex((item2) => item2.chapter === item.chapter);
                                        if (index < 0) {
                                            acc.push({
                                                chapter: item.chapter,
                                                chapter_alias_url: item.chapter_alias_url,
                                            });
                                        }
                                        return acc;
                                    }, []);
                                redis.setAsync(`QUIZ:WEB:CLASS:CHAPTERS:${classKey}:${subjectKey}:${languageKey}`, JSON.stringify(availableChapters), "Ex", 60 * 60 * 24 * 7);
                                // console.log(`Available chapters for ${subjectKey} are ${availableChapters}`);
                                for (const chapterKey in chaptersMapping) {
                                    if (chaptersMapping[chapterKey]) {
                                        const questions = _.groupBy(chaptersMapping[chapterKey], "question_id");
                                        const questionIdsArray = Object.keys(questions);
                                        if (questionIdsArray.length) {
                                            redis.setAsync(`QUIZ:WEB:QUESTIONS:${classKey}:${subjectKey}:${languageKey}:${chapterKey}`, JSON.stringify(questionIdsArray.slice(0, 200)), "Ex", 60 * 60 * 24 * 7);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                job.progress(parseInt(((i + 1) / availableClasses.length) * 100));
                i++;
            }
        }
        job.progress(100);
    } catch (err) {
        console.log(err);
        return { err };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 4 */3 * *", // * Every 3 days at 4 AM
};
