/* eslint-disable no-await-in-loop */
const sharp = require("sharp");
const { promisify } = require("util");
const fs = require("fs");
const {
    config, aws, mysql, puppeteer,
} = require("../../modules");

const readFileAsync = promisify(fs.readFile);

const settings = {
    templates: {
        replay_class: "./src/replayclass-thumbnail-generation/templates/layout_old_new_template_fix.html",
    },
    s3: {
        bucketName: "doubtnut-static",
    },
};

async function getReplayClasses() {
    const sql = "SELECT a.id as detail_id, d.class, a.live_at,hour(a.live_at) as hour_class, a.subject as subject_class, a.liveclass_course_id, b.topic,a.chapter, b.resource_reference, c.name as faculty_name ,c.image_url as faculty_image,c.gender,concat(upper(left(c.name,1)),lower(right(substring_index(c.name,' ',1),length(substring_index(c.name,' ',1))-1)),' ', case when upper(c.gender) = 'MALE' then 'Sir' else 'Maam' end) as faculty_nickname,d.title,d.locale from liveclass_course_resources as b left join liveclass_course_details as a on a.id=b.liveclass_course_detail_id left join dashboard_users as c on a.faculty_id=c.id left JOIN liveclass_course as d on a.liveclass_course_id=d.id left join course_details_liveclass_course_mapping as e on a.liveclass_course_id = e.liveclass_course_id  where ((e.class_type in ('LIVE_CLASS_REPLAY') and b.resource_type in (1,8))) and date(a.live_at)=CURRENT_DATE and a.is_replay = 0 ORDER BY a.id DESC";
    return mysql.pool.query(sql).then(([res]) => res);
}

async function uploadToS3(objectParams) {
    try {
        const result = await aws.s3.putObject(objectParams).promise();
        return result;
    } catch (e) {
        console.log(e);
    }
}
function escapeRegExp(string) {
    return string.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/* Define function to find and replace specified term with replacement string */
function replaceAll(str, term, replacement) {
    return str.replace(new RegExp(escapeRegExp(term), "g"), replacement);
}

async function generateThumbnail(browser, template, max = 0) {
    try {
        const page = await browser.newPage();
        await page.setViewport({
            width: 670,
            height: 378,
            deviceScaleFactor: 1,
        });

        await page.setContent(template, { waitUntil: "networkidle2", timeout: 0 });

        // * png and webp image buffers
        const pngImage = await page.screenshot({ type: "png" });
        const webpImage = await sharp(pngImage).webp().toBuffer();

        await page.close();

        return { pngImage, webpImage };
    } catch (err) {
        console.error(err);
        if (max !== 3) {
            return generateThumbnail(browser, template, max + 1);
        }
        return { err };
    }
}

async function start(job) {
    try {
        const browser = await puppeteer.getBrowser();

        const { templates, s3 } = settings;

        const allTopics = await getReplayClasses();
        for (let i = 0; i < allTopics.length; i++) {
            const { detail_id } = allTopics[i];
            const class_id = allTopics[i].class;
            const subject = allTopics[i].subject_class;
            const subject_lower = subject.toLowerCase().replace(" ", "_");
            const question_id = allTopics[i].resource_reference;
            const { faculty_image } = allTopics[i];
            const faculty_name = allTopics[i].faculty_nickname;
            let topic = allTopics[i].topic.replace(" (recorded)", "");
            topic = topic.substring(0, 100);
            topic = replaceAll(topic, "||", "<br>");
            topic = replaceAll(topic, "|", "<br>");

            const { locale } = allTopics[i];
            let template_name = "";

            const locale_lower = locale.toLowerCase().substr(0, 2);

            if (class_id == 14) {
                if (faculty_image == null) {
                    template_name = templates.replay_class;
                } else {
                    template_name = templates.replay_class;
                }
            } else if (class_id < 14) {
                if (faculty_image == null) {
                    template_name = templates.replay_class;
                } else {
                    template_name = templates.replay_class;
                }
            }

            let template = await readFileAsync(template_name, "utf8");
            // template =  template.replace('$',img_link)
            template = template.replace("###SUB_LOWER###", subject_lower);
            template = template.replace("###SUB_LOWER###", subject_lower);
            template = template.replace("###LOCALE_LOWER###", locale_lower);
            template = template.replace("###TOPIC###", topic);
            template = template.replace("###CLASS###", class_id);
            template = template.replace("###FACNAME###", faculty_name);
            template = template.replace("###FACIMAGE###", faculty_image);
            // template =  template.replace('##BGIMAGE##', 'https://d10lpgp6xz60nq.cloudfront.net/images/etoos/thumbnail-background/'+subject+'_S.png')

            const { pngImage, webpImage, err } = await generateThumbnail(browser, template, 0);

            if (err) {
                throw new Error(err);
            }

            // * png and webp image names
            const pngImageName = `notif-thumb-${detail_id}-${class_id}-${question_id}.png`;
            const pngImageNameByQuestionIdOnly = `${question_id}.png`;
            const webpImageName = `notif-thumb-${detail_id}-${class_id}-${question_id}.webp`;
            const webpImageNameByQuestionIdOnly = `${question_id}.webp`;

            // * png and webp s3 prefix
            const pngKeyName = `q-thumbnail/${pngImageName}`;
            const pngKeyNameByQuestionIdOnly = `q-thumbnail/${pngImageNameByQuestionIdOnly}`;
            const webpKeyName = `q-thumbnail/${webpImageName}`;
            const webpKeyNameByQuestionIdOnly = `q-thumbnail/${webpImageNameByQuestionIdOnly}`;

            // * png and webp object params
            const pngObjectParams = {
                Bucket: s3.bucketName,
                Key: pngKeyName,
                Body: pngImage,
                ContentType: "image/png",
            };
            const pngObjectParamsByQuestionIdOnly = {
                Bucket: s3.bucketName,
                Key: pngKeyNameByQuestionIdOnly,
                Body: pngImage,
                ContentType: "image/png",
            };

            const webpObjectParams = {
                Bucket: s3.bucketName,
                Key: webpKeyName,
                Body: webpImage,
                ContentType: "image/webp",
            };

            const webpObjectParamsByQuestionIdOnly = {
                Bucket: s3.bucketName,
                Key: webpKeyNameByQuestionIdOnly,
                Body: webpImage,
                ContentType: "image/webp",
            };

            // * png and webp upload to s3
            await uploadToS3(pngObjectParams);
            await uploadToS3(pngObjectParamsByQuestionIdOnly);
            await uploadToS3(webpObjectParams);
            await uploadToS3(webpObjectParamsByQuestionIdOnly);

            console.log(`Image pushed to s3 at ${config.staticCloudfrontCDN}${pngKeyName} location in ${settings.s3.bucketName}`);
            console.log(`Image pushed to s3 at ${config.staticCloudfrontCDN}${pngKeyNameByQuestionIdOnly} location in ${settings.s3.bucketName}`);
            console.log(`Image pushed to s3 at ${config.staticCloudfrontCDN}${webpKeyName} location in ${settings.s3.bucketName}`);
            console.log(`Image pushed to s3 at ${config.staticCloudfrontCDN}${webpKeyNameByQuestionIdOnly} location in ${settings.s3.bucketName}`);

            await job.progress(parseInt(((i + 1) / allTopics.length) * 100));
        }
        await job.progress(100);
        return { err: null, data: null };
    } catch (err) {
        console.error(err);
        return { err };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "56 0,5,12,15 * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
