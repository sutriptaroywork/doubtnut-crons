/* eslint-disable no-await-in-loop */
const sharp = require("sharp");
const { promisify } = require("util");
const fs = require("fs");

const {
    config, aws, mysql, puppeteer,
} = require("../../modules");

const readFileAsync = promisify(fs.readFile);

const OUTPUT_WIDTH = 640;
const PNG_COMPRESSION_OPTIONS = {
    compressionLevel: 9,
    quality: 50,
    effort: 10,
    adaptiveFiltering: true,
};

const WEBP_COMPRESSION_OPTIONS = {
    effort: 6,
    quality: 50,
    adaptiveFiltering: true,
};

const settings = {
    templates: {
        live_class_govt_without_img: "./src/liveclass-thumbnail-generation/templates/live-class-govt-without-img.html",
        live_class_govt: "./src/liveclass-thumbnail-generation/templates/live-class-govt.html",
        live_class_without_img: "./src/liveclass-thumbnail-generation/templates/live-class-without-img.html",
        live_class: "./src/liveclass-thumbnail-generation/templates/live-class.html",
    },
    backgroundImages: {
        live_class_bk: "./src/liveclass-thumbnail-generation/background/images/live-class-bk.png",
        live_class_comment_bk_2: "./src/liveclass-thumbnail-generation/background/images/live-class-comment-bk-2.png",
        live_class_play_btn_2: "./src/liveclass-thumbnail-generation/background/images/live-class-play-btn-2.png",
        live_class_watch_now_2: "./src/liveclass-thumbnail-generation/background/images/live-class-watch-now-2.png",
    },
    s3: {
        bucketName: config.staticBucket,
    },
};

async function getAllTopics() {
    const sql = "SELECT a.id as detail_id, d.class, a.live_at,hour(a.live_at) as hour_class, minute(a.live_at) as minute_class, a.subject as subject_class, a.liveclass_course_id, a.chapter, b.resource_reference, c.name as faculty_name ,c.image_url as faculty_image,d.title from liveclass_course_resources as b left join liveclass_course_details as a on a.id=b.liveclass_course_detail_id left join dashboard_users as c on a.faculty_id=c.id left JOIN liveclass_course as d on a.liveclass_course_id=d.id left join course_details_liveclass_course_mapping as e on a.liveclass_course_id = e.liveclass_course_id  where ((e.vendor_id = 1 and b.resource_type = 4) or (e.vendor_id = 2 and b.resource_type = 1) or (e.class_type in ('DN_VOD') and b.resource_type in (1,8))) and date(a.live_at)=CURRENT_DATE and a.is_replay = 0 ORDER BY a.live_at  DESC";
    return mysql.pool.query(sql).then(([res]) => res);
}

async function putImage(objectParams) {
    const result = await aws.s3.putObject(objectParams)
        .promise();
    console.log(`Image pushed to s3 at ${objectParams.Key} location in ${objectParams.Bucket}`);
    return result;
}

async function uploadToS3(objectParams) {
    try {
        const searchExistingObject = {
            Key: objectParams.Key,
            Bucket: objectParams.Bucket,
        };
        const existingS3Object = await aws.s3.getObject(searchExistingObject)
            .promise(); // Raises an exception {Object} with `name` = 'NoSuchKey' if the object doesn't exist.
        console.log(`Image already exists ${objectParams.Key} location in ${objectParams.Bucket}`);
        if (existingS3Object.ContentLength === 0) {
            console.log(`Image size 0 at ${objectParams.Key} location in ${objectParams.Bucket}. Uploading again...`);
            await putImage(objectParams);
        }
    } catch (e) {
        if (e.name === "NoSuchKey") {
            // eslint-disable-next-line no-return-await
            return await putImage(objectParams);
        }
        console.log(e);
    }
}

async function generateThumbnail(browser, template, max = 0) {
    try {
        const page = await browser.newPage();
        await page.setViewport({
            width: 970,
            height: 465,
            deviceScaleFactor: 1,
        });

        await page.setContent(template, { waitUntil: "networkidle0", timeout: 0 });

        // * png and webp image buffers
        const originalImage = await page.screenshot({ type: "png" });
        const resizedPngImage = await sharp(originalImage).resize(OUTPUT_WIDTH).png(PNG_COMPRESSION_OPTIONS).toBuffer();
        const resizedWebpImage = await sharp(originalImage).resize(OUTPUT_WIDTH).webp(WEBP_COMPRESSION_OPTIONS).toBuffer();

        await page.close();

        return { pngImage: resizedPngImage, webpImage: resizedWebpImage, err: null };
    } catch (err) {
        console.error(err);
        if (max !== 3) {
            return generateThumbnail(browser, template, max + 1);
        }
        return { pngImage: null, webpImage: null, err };
    }
}

async function start(job) {
    try {
        const live_class_bk = await readFileAsync(settings.backgroundImages.live_class_bk, "base64");
        const live_class_comment_bk_2 = await readFileAsync(settings.backgroundImages.live_class_comment_bk_2, "base64");
        const live_class_play_btn_2 = await readFileAsync(settings.backgroundImages.live_class_play_btn_2, "base64");
        const live_class_watch_now_2 = await readFileAsync(settings.backgroundImages.live_class_watch_now_2, "base64");

        const browser = await puppeteer.getBrowser();

        const { templates, s3 } = settings;

        const allTopics = await getAllTopics();
        for (let i = 0; i < allTopics.length; i++) {
            const {
                detail_id: detailId,
                class: classId,
                subject_class: subject,
                chapter: lecture,
                resource_reference: questionId,
                faculty_image: facultyImage,
                faculty_name: facultyName,
                title,
                hour_class: hourClass,
                minute_class: minuteClass,
            } = allTopics[i];

            let templateName = "";

            const timeClass1 = hourClass % 12 || 12; // Adjust hours
            const timeClass2 = hourClass < 12 ? "AM" : "PM"; // Set AM/PM
            const classTime = !minuteClass ? `${timeClass1} ${timeClass2}` : `${timeClass1}:${minuteClass} ${timeClass2}`;

            if (classId === 14) {
                if (facultyImage == null) {
                    templateName = templates.live_class_govt_without_img;
                } else {
                    templateName = templates.live_class_govt;
                }
            } else if (classId < 14) {
                if (facultyImage == null) {
                    templateName = templates.live_class_without_img;
                } else {
                    templateName = templates.live_class;
                }
            }

            let template = await readFileAsync(templateName, "utf8");

            template = template.replace("#top#", title);
            template = template.replace("#time#", classTime);
            template = template.replace("#class#", classId);
            template = template.replace("#subject#", subject);
            template = template.replace("#chapter#", lecture);
            template = template.replace("#expert#", facultyName);
            template = template.replace("#image#", facultyImage);
            template = template.replace(/#staticCDN#/g, config.staticCDN);

            template = template.replace(/##live-class-bk##/g, live_class_bk);
            template = template.replace(/##live-class-comment-bk-2##/g, live_class_comment_bk_2);
            template = template.replace(/##live-class-play-btn-2##/g, live_class_play_btn_2);
            template = template.replace(/##live-class-watch-now-2##/g, live_class_watch_now_2);

            const { pngImage, webpImage, err } = await generateThumbnail(browser, template, 0);

            if (err) {
                throw new Error(err);
            }

            // * png and webp image names
            const pngImageName = `notif-thumb-${detailId}-${classId}-${questionId}.png`;
            const webpImageName = `notif-thumb-${detailId}-${classId}-${questionId}.webp`;
            // * png and webp image names
            const questionPngImageName = `${questionId}.png`;
            const questionWebpImageName = `${questionId}.webp`;

            // * png and webp s3 prefix
            const pngKeyName = `q-thumbnail/${pngImageName}`;
            const webpKeyName = `q-thumbnail/${webpImageName}`;
            // * png and webp s3 prefix
            const questionPngKeyName = `q-thumbnail/${questionPngImageName}`;
            const questionWebpKeyName = `q-thumbnail/${questionWebpImageName}`;

            // * png and webp object params
            const pngObjectParams = {
                Bucket: s3.bucketName,
                Key: pngKeyName,
                Body: pngImage,
                ContentType: "image/png",
            };

            const webpObjectParams = {
                Bucket: s3.bucketName,
                Key: webpKeyName,
                Body: webpImage,
                ContentType: "image/webp",
            };

            // * png and webp object params
            const questionPngObjectParams = {
                Bucket: s3.bucketName,
                Key: questionPngKeyName,
                Body: pngImage,
                ContentType: "image/png",
            };

            const questionWebpObjectParams = {
                Bucket: s3.bucketName,
                Key: questionWebpKeyName,
                Body: webpImage,
                ContentType: "image/webp",
            };

            // * png and webp upload to s3
            await uploadToS3(pngObjectParams);
            await uploadToS3(questionPngObjectParams);
            await uploadToS3(webpObjectParams);
            await uploadToS3(questionWebpObjectParams);

            await job.progress(parseInt(((i + 1) / allTopics.length) * 100));
            console.log("%d of %d done", i, allTopics.length);
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
    cron: "10 0,5,13,15 * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
