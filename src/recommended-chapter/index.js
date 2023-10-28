const _ = require("lodash");
const { redis } = require("../../modules/index");
const { mysql } = require("../../modules");

async function getLatestClasses() {
    const sql = `SELECT  lc.class,lc.locale,lc.course_exam,lcd.subject,resource_reference
        from classzoo1.liveclass_course_resources lcr
        left join classzoo1.liveclass_course_details lcd on lcd.id = lcr.liveclass_course_detail_id
        left join classzoo1.liveclass_course lc on lc.id = lcr.liveclass_course_id
        where resource_type in (1,4,8) and is_replay = 0 
        and lc.is_free = 1 
        and lc.course_exam  in ('BOARDS', 'IIT', 'NEET')
        and lc.locale in ('HINDI', 'ENGLISH')
        and date(live_at) >= current_date - 3 and date(live_at) <= current_date 
        group by 1,2,3,4,5`;
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function start(job) {
    try {
        job.progress(0);
        const classesData = await getLatestClasses();
        const groupedByClass = _.groupBy(classesData, "class");
        Object.keys(groupedByClass).map((e) => {
            const groupedByLocale = _.groupBy(groupedByClass[e], "locale");
            Object.keys(groupedByLocale).map((l) => {
                const groupedByCategory = _.groupBy(groupedByLocale[l], "course_exam");
                groupedByLocale[l] = groupedByCategory;
                return true;
            });
            groupedByClass[e] = groupedByLocale;
            return true;
        });

        await Promise.all(Object.keys(groupedByClass).map(async (eClass) => {
            await Promise.all(Object.keys(groupedByClass[eClass]).map(async (locale) => {
                await Promise.all(Object.keys(groupedByClass[eClass][locale]).map(async (category) => {
                    const data = groupedByClass[eClass][locale][category].slice(0, 10).map((d) => {
                        const temp = {
                            subject: d.subject,
                            id: d.resource_reference,
                        };
                        return temp;
                    });
                    console.log(data, eClass, locale, category);
                    await redis.setAsync(`u:rec:${eClass}_${locale}_${category}`, JSON.stringify(data), "Ex", 60 * 60 * 6);
                    return true;
                }));
                return true;
            }));
            return true;
        }));
    } catch (e) {
        console.error(e);
    }
    job.progress(100);
    return true;
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 */2 * * *", // Every 2nd hour
};
