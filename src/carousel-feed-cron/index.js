/* eslint-disable eqeqeq */
/* eslint-disable no-await-in-loop */
const moment = require("moment");
const _ = require("lodash");
const md5 = require("md5");

const {
    mysql, config, getMongoClient,
} = require("../../modules");

const {
    Data,
    getBarColorForRecentclassHomepage,
    LiveclassData,
    buildStaticCdnUrl,
    generateVideoResourceObject,
    showWhatsapp,
    getTopperTestimonialWidget,
    getTextColor,
} = require("./helper");

async function getNameAndValueByBucket(bucket) {
    const sql = "select name,value from dn_property where bucket = ? and is_active = 1 order by priority";
    return mysql.pool.query(sql, [bucket]).then((res) => res[0]);
}
const TrickyQuestionsCache = {};

const classesArray = ["6", "7", "8", "9", "10", "11", "12", "14"];

const secondarySQLDataMap = {};
// Function Fetching Carousel Data

async function getSecondaryData(sql) {
    const sqlHASH = md5(sql);
    if (secondarySQLDataMap[sqlHASH]) {
        return secondarySQLDataMap[sqlHASH];
    }
    const itemsData = await mysql.pool.query(sql).then((res) => res[0]);
    secondarySQLDataMap[sqlHASH] = itemsData;

    return itemsData;
}

async function getAdhocWidget(carousel) {
    let androidWidget = {};
    if (carousel.data_type === "carousel_list") {
        const itemsSql = carousel.secondary_data;
        const itemsData = await getSecondaryData(itemsSql);
        if (itemsData.length > 0) {
            const widgetItems = itemsData.map((eachItem) => {
                const it = { ...eachItem };
                it.deeplink = it.action_deeplink;
                return it;
            });
            androidWidget = {
                widget_type: "carousel_list",
                layout_config: {
                    margin_top: 2,
                    margin_left: 0,
                    margin_right: 0,
                    margin_bottom: 0,
                },
                widget_data: {
                    title: carousel.title,
                    auto_scroll_time_in_sec: itemsData[0].auto_scroll_time_sec || 3,
                    full_width_cards: true,
                    items: widgetItems,
                    carousel_id: carousel.id,
                },
                order: carousel.caraousel_order,
            };
        }
    } else if (carousel.data_type === "banner_image") {
        androidWidget = {
            widget_type: "banner_image",
            widget_data: {
                image_url: carousel.image_url,
                deeplink: carousel.action_deeplink,
                card_ratio: "16: 9",
                card_width: "1.0",
                carousel_id: carousel.id,
            },
            order: carousel.caraousel_order,
        };
    } else if (carousel.data_type === "widget_popular_course") {
        androidWidget = {
            widget_type: "widget_popular_course",
            widget_data: {
                image_url: carousel.image_url,
                deeplink: carousel.deeplink,
                card_ratio: "16: 9",
                card_width: "1.0",
                carousel_id: carousel.id,
            },
            order: carousel.caraousel_order,
        };
    } else if (carousel.data_type === "daily_quiz") {
        const testId = JSON.parse(carousel.secondary_data);
        const testSql = `SELECT  * FROM testseries WHERE is_active = 1 AND test_id = ${testId}`;

        const testData = await mysql.pool.query(testSql).then((res) => res[0]);
        const testWidgetData = testData[0];

        testWidgetData.can_attempt = false;
        testWidgetData.can_attempt_prompt_message = "";
        testWidgetData.test_subscription_id = "";
        testWidgetData.in_progress = false;
        testWidgetData.attempt_count = 0;
        testWidgetData.last_grade = "";
        testWidgetData.type = "quiz";
        testWidgetData.image_url = `${config.staticCDN}images/quiz_sample.jpeg`;
        testWidgetData.button_text = "Go To";
        testWidgetData.button_text_color = "#000000";
        testWidgetData.button_bg_color = "#ffffff";
        testWidgetData.id = testWidgetData.test_id.toString();
        testWidgetData.can_attempt = true;
        testWidgetData.subscriptiondata = [];
        testWidgetData.carousel_id = carousel.id;

        androidWidget = {
            widget_data: testWidgetData,
            widget_type: "daily_quiz",
            layout_config: {
                margin_top: 16,
                margin_left: 0,
                margin_right: 0,
            },
            order: carousel.caraousel_order,
        };
    } else if (carousel.data_type === "whatsapp") {
        androidWidget = {
            widget_type: "whatsapp",
            layout_config: {
                margin_top: 16,
                bg_color: "#ffffff",
            },
            order: carousel.caraousel_order,
            widget_data: {
                type: "WHATSAPP_ASK",
                data_type: "card",
                title: carousel.title,
                image_url: carousel.image_url,
                deeplink: carousel.action_deeplink,
                show_view_all: 0,
                items: [],
                widget_type: "whatsapp",
                carousel_type: "carousel",
                carousel_id: carousel.id,
            },
        };
    } else if (carousel.data_type === "widget_autoplay") {
        const itemsSql = carousel.secondary_data;
        const itemsData = await getSecondaryData(itemsSql);

        if (itemsData.length > 0) {
            const widgetItems = itemsData.map((eachItem) => {
                const it = { ...eachItem };
                it.video_resource = {
                    resource: it.video_resource,
                    auto_play_duration: it.auto_play_duration,
                };
                delete it.auto_play_duration;
                it.default_mute = Boolean(it.default_mute);
                it.auto_play = Boolean(it.auto_play);

                return {
                    type: "video_banner_autoplay_child",
                    data: it,
                };
            });

            androidWidget = {
                widget_type: "widget_autoplay",
                widget_data: {
                    title: carousel.title,
                    full_width_cards: true,
                    default_mute: Boolean(itemsData[0].default_mute),
                    auto_play: Boolean(itemsData[0].auto_play),
                    id: carousel.id,
                    items: widgetItems,
                    carousel_id: carousel.id,
                },
                order: carousel.caraousel_order,
            };
        }
    } else if (carousel.data_type === "image_card_group") {
        const itemsSql = carousel.secondary_data;
        const itemsData = await getSecondaryData(itemsSql);

        if (itemsData.length > 0) {
            const groupedItemsData = _.groupBy(itemsData, "group_title");
            const cardGroups = Object.keys(groupedItemsData).map((eachtitle) => {
                const groupData = groupedItemsData[eachtitle];

                const groupDataItems = groupData.map((eachitem) => {
                    const it = { ...eachitem };
                    it.show_whatsapp = true;
                    it.show_video = false;
                    it.card_width = "2.5x";
                    it.aspect_ratio = "";

                    delete it.overall_deeplink;
                    delete it.group_id;
                    delete it.group_title;

                    return it;
                });
                const groupItem = {
                    group_id: groupData[0].group_id,
                    group_title: groupData[0].group_title,
                    deeplink: groupData[0].overall_deeplink,
                    items: groupDataItems,
                };
                return groupItem;
            });
            androidWidget = {
                widget_type: "image_card_group",
                widget_data: {
                    title: carousel.title,
                    card_groups: cardGroups,
                    carousel_id: carousel.id,
                },
                order: carousel.caraousel_order,
            };
        }
    } else if (carousel.data_type === "mock_test") {
        const itemsSql = carousel.secondary_data;
        const itemsData = await getSecondaryData(itemsSql);

        if (itemsData.length > 0) {
            const widgetData = itemsData[0];

            widgetData.title = widgetData.test_title;
            widgetData.sub_title = widgetData.test_sub_title;
            widgetData.sub_title_icon = `${config.staticCDN}engagement_framework/6674B126-E006-859A-8978-26D7BBB7507A.webp`;
            widgetData.is_background_image = true;

            androidWidget = {
                widget_type: "widget_parent",
                layout_config: {
                    margin_top: 16,
                    bg_color: "#FFFFFF",
                },
                order: carousel.caraousel_order,
                widget_data: {
                    carousel_id: carousel.id,
                    // "link_text": itemsData[0].link_test,
                    // "deeplink": itemsData[0].link_text_deeplink,
                    title: carousel.title,
                    title_text_size: "16",
                    background_color: "#eaf3f9",
                    scroll_direction: "vertical",
                    is_title_bold: true,
                    items: [{
                        type: "widget_recommended_test",
                        data: widgetData,
                        order: carousel.caraousel_order,
                    }],
                },
            };
        }
    } else if (carousel.data_type === "widget_course_v4") {
        const itemsSql = carousel.secondary_data;
        const itemsData = await getSecondaryData(itemsSql);

        if (itemsData.length > 0) {
            const widgetItems = itemsData.map((eachItem) => {
                const it = { ...eachItem };
                it.default_mute = Boolean(it.default_mute);
                it.tag_data = [];
                if (it.tag_title_1) {
                    it.tag_data.push({
                        title: it.tag_title_1,
                        bg_color: it.tag_bg_color_1 || "#e34c4c",
                    });
                }
                if (it.tag_title_2) {
                    it.tag_data.push({
                        title: it.tag_title_2,
                        bg_color: it.tag_bg_color_2 || "#e34c4c",
                    });
                }
                if (it.tag_title_3) {
                    it.tag_data.push({
                        title: it.tag_title_3,
                        bg_color: it.tag_bg_color_3 || "#e34c4c",
                    });
                }
                return it;
            });
            androidWidget = {
                type: "widget_course_v4",
                divider_config: {
                    background_color: "#e4ecf1",
                    width: 4,
                },
                extra_params: {},
                order: carousel.caraousel_order,
                data: {
                    title: carousel.title,
                    auto_scroll_time_in_sec: 3,
                    items: widgetItems,
                },
            };
        }
    } else if (carousel.data_type === "teacher_channel_list") {
        const itemsSql = carousel.secondary_data;
        const itemsData = await getSecondaryData(itemsSql);

        if (itemsData.length > 0) {
            const widgetItems = itemsData.map((eachItem) => {
                const it = { ...eachItem };
                it.id = it.teacher_id;
                it.type = it.teacher_type;
                it.experience = it.experience_years;
                it.deeplink = it.button_deeplink;

                it.background_color = it.background_color || "#FFC5B2";
                it.card_width = it.card_width || "2.0";
                it.card_ratio = it.card_ratio || "16:19";
                return it;
            });
            androidWidget = {
                type: "teacher_channel_list",
                layout_config: {
                    margin_top: 16,
                    bg_color: "#ffffff",
                },
                order: carousel.caraousel_order,
                data: {
                    title: carousel.title,
                    items: widgetItems,
                },
            };
        }
    } else if (carousel.data_type == "course_extra_marks") {
        const itemsSql = carousel.secondary_data;
        let itemsData = await getSecondaryData(itemsSql);
        if (!itemsData.length) {
            itemsData = null;
        }
        androidWidget = {
            widget_type: "widget_parent",
            layout_config: {
                margin_top: 16,
                bg_color: "#FFFFFF",
            },
            order: carousel.caraousel_order,
            widget_data: {
                carousel_id: carousel.id,
                data_type: "course_extra_marks",
                is_et_reorder: true,
                items: [{
                    data: itemsData,
                    order: carousel.caraousel_order,
                }],
            },
        };
    } else if (carousel.data_type == "testimonial") {
        const itemsSql = carousel.secondary_data;
        const itemsData = await getSecondaryData(itemsSql);
        if (itemsData.length) {
            androidWidget = getTopperTestimonialWidget({
                carouselsData: carousel,
                result: itemsData,
                locale: carousel.locale,
            });
        }
    } else if (carousel.data_type === "course_categories") {
        const itemsSql = carousel.secondary_data;
        const itemsData = await getSecondaryData(itemsSql);

        if (itemsData.length > 0) {
            const allCategories = [...itemsData];
            const categoryItems = [];
            for (let i = 0; i < allCategories.length; i++) {
                categoryItems.push({
                    title: allCategories[i].id,
                    image_url: LiveclassData.getcategoryIcons(allCategories[i].id, 1000),
                    id: allCategories[i].id,
                    deeplink: `doubtnutapp://course_category?category_id=${allCategories[i].id === "NDA" ? "DEFENCE/NDA/NAVY_CT" : allCategories[i].id}`,
                });
            }

            if (categoryItems.length) {
                androidWidget = {
                    widget_type: "widget_parent",
                    order: carousel.caraousel_order,
                    widget_data: {
                        carousel_id: carousel.id,
                        title: carousel.title,
                        is_title_bold: true,
                        background_color: "#eaf3f9",
                        items: [{
                            type: "widget_autoplay",
                            data: {
                                category_items: categoryItems,
                            },
                        }],
                    },
                };
            }
        }
    }
    return androidWidget;
}

async function getqidThumbnail(qid) {
    const sql = "select question_id from new_thumbnail_experiment where question_id in (?) and is_active=1 group by question_id";
    return mysql.pool.query(sql, [qid]).then((res) => res[0]);
}

async function qidThumbnailCheck(data) {
    const qidList = data.map((x) => x.id);
    const thumbnailData = await getqidThumbnail(qidList);
    const thumbnailQidMapping = {};
    for (let i = 0; i < thumbnailData.length; i++) {
        thumbnailQidMapping[thumbnailData[i].question_id] = true;
    }
    return thumbnailQidMapping;
}

async function getAdhocChildWidget(carousel) {
    let androidWidget = {};
    const [widgetType, widgetChildType] = carousel.data_type.split("#!#");
    if (widgetType === "widget_parent" && widgetChildType === "live_class_carousel_card_2") {
        const itemsSql = carousel.secondary_data;
        const itemsData = await getSecondaryData(itemsSql);

        if (itemsData.length > 0) {
            const thumbnailData = await qidThumbnailCheck(itemsData);
            const widgetItems = itemsData.map((eachItem) => {
                const it = { ...eachItem };
                it.button = {
                    text: it["GO TO CHAPTER"],
                    deeplink: it.button_deeplink,
                };
                it.is_live = Boolean(it.is_live);
                it.is_vip = Boolean(it.is_vip);
                it.is_premium = Boolean(it.is_premium);
                it.show_reminder = Boolean(it.show_reminder);
                it.state = 2;
                it.live_at = new Date(it.live_at).valueOf();
                it.set_width = true;

                if (it.thumbnail) {
                    it.image_bg_card = !thumbnailData[it.id] ? `${config.staticCDN}q-thumbnail/${it.id}.webp` : it.thumbnail;
                    it.subject = "";
                    it.title = "";
                    it.title1 = "";
                    it.title2 = "";
                    it.image_url = "";
                } else {
                    it.image_bg_card = buildStaticCdnUrl(LiveclassData.getBgImage(it.subject.toLowerCase()));
                }
                // it.image_bg_card = it.thumbnail || buildStaticCdnUrl(LiveclassData.getBgImage(it.subject.toLowerCase()));
                it.color = getBarColorForRecentclassHomepage(it.subject);
                it.lock_state = 0;
                it.topic = "";
                it.students = 13822;
                it.interested = 13822;
                it.card_width = it.card_width || "1.1";
                it.card_ratio = it.card_ratio || "16:9";
                const textColors = getTextColor(it.subject.toLowerCase());
                it.text_color_primary = textColors.text_color_primary || "#ffffff";
                it.text_color_secondary = textColors.text_color_secondary || "#ffffff";
                it.text_color_title = textColors.text_color_title || "#ffffff";
                return {
                    type: "live_class_carousel_card_2",
                    data: it,
                };
            });

            androidWidget = {
                widget_type: "widget_parent",
                layout_config: {
                    margin_top: 16,
                    bg_color: "#FFFFFF",
                },
                order: carousel.caraousel_order,
                widget_data: {
                    title: carousel.title,
                    is_title_bold: true,
                    items: widgetItems,
                    carousel_id: carousel.id,
                    scroll_direction: carousel.scroll_type,
                },
            };
            if (carousel.scroll_type == "vertical") {
                androidWidget.widget_data.link_text = carousel.locale == "hi" ? "सभी देखें" : "View All";
                androidWidget.widget_data.deeplink = `doubtnutapp://paginated_bottom_sheet_widget?id=homepage_vertical&tab_id=${carousel.id}`;
            }
        }
    } else if (widgetType === "widget_parent_tab" && widgetChildType === "live_class_carousel_card_2") {
        const itemsSql = carousel.secondary_data;
        const itemsData = await getSecondaryData(itemsSql);

        if (itemsData.length > 0) {
            const thumbnailData = await qidThumbnailCheck(itemsData);
            const widgetItems = itemsData.map((eachItem) => {
                const it = { ...eachItem };
                it.button = {
                    text: it.button_text,
                    deeplink: it.button_deeplink,
                };
                it.id = parseInt(it.id);
                it.is_live = Boolean(it.is_live);
                it.is_vip = Boolean(it.is_vip);
                it.is_premium = Boolean(it.is_premium);
                it.show_reminder = Boolean(it.show_reminder);
                it.state = 2;
                it.live_at = new Date(it.live_at).valueOf();
                it.set_width = true;

                if (it.thumbnail) {
                    it.image_bg_card = !thumbnailData[it.id] ? `${config.staticCDN}q-thumbnail/${it.id}.webp` : it.thumbnail;
                    it.subject = "";
                    it.title1 = "";
                    it.title2 = "";
                    it.image_url = "";
                } else {
                    it.image_bg_card = buildStaticCdnUrl(LiveclassData.getBgImage(it.subject.toLowerCase()));
                }
                it.color = getBarColorForRecentclassHomepage(it.subject);
                it.lock_state = 0;
                it.set_width = true;
                it.topic = "";
                it.students = 13822;
                it.interested = 13822;
                it.card_width = it.card_width || "1.1";
                it.card_ratio = it.card_ratio || "16:9";
                const textColors = getTextColor(it.subject.toLowerCase());
                it.text_color_primary = textColors.text_color_primary || "#ffffff";
                it.text_color_secondary = textColors.text_color_secondary || "#ffffff";
                it.text_color_title = textColors.text_color_title || "#ffffff";
                return it;
            });
            const groupedData = _.groupBy(widgetItems, "title");

            const tabs = [];
            for (const key in groupedData) {
                if (groupedData[key]) {
                    tabs.push({
                        key: `${key}`,
                        title: groupedData[key][0].title,
                        is_selected: false,
                    });
                    const widgeted = groupedData[key].map((eit) => ({
                        type: "live_class_carousel_card_2",
                        data: eit,
                    }));
                    groupedData[key] = widgeted;
                }
            }

            if (tabs.length) {
                tabs[0].is_selected = true;
            }

            androidWidget = {
                widget_type: "widget_parent_tab",
                layout_config: {
                    margin_top: 16,
                    bg_color: "#FFFFFF",
                },
                order: carousel.caraousel_order,
                widget_data: {
                    title: carousel.title,
                    is_title_bold: true,
                    items: groupedData,
                    tabs,
                    carousel_id: carousel.id,
                    scroll_direction: carousel.scroll_type,
                },
            };
            if (carousel.scroll_type == "vertical") {
                androidWidget.widget_data.link_text = carousel.locale == "hi" ? "सभी देखें" : "View All";
                androidWidget.widget_data.deeplink = `doubtnutapp://paginated_bottom_sheet_widget?id=homepage_vertical&tab_id=${carousel.id}`;
            }
        }
    } else if (widgetType === "widget_parent" && widgetChildType === "widget_recommended_test") {
        const itemsSql = carousel.secondary_data;
        const itemsData = await getSecondaryData(itemsSql);

        if (itemsData.length > 0) {
            const widgetItems = itemsData.map((eachItem) => {
                const it = { ...eachItem };
                it.title = it.test_title;
                it.sub_title = it.test_sub_title;
                it.sub_title_icon = `${config.staticCDN}engagement_framework/6674B126-E006-859A-8978-26D7BBB7507A.webp`;
                it.is_background_image = true;
                return {
                    type: "widget_recommended_test",
                    data: it,
                };
            });
            androidWidget = {
                widget_type: "widget_parent",
                layout_config: {
                    margin_top: 16,
                    bg_color: "#FFFFFF",
                },
                order: carousel.caraousel_order,
                widget_data: {
                    carousel_id: carousel.id,
                    link_text: itemsData[0].link_test,
                    deeplink: itemsData[0].link_text_deeplink,
                    title: carousel.title,
                    title_text_size: "16",
                    background_color: "#eaf3f9",
                    scroll_direction: "vertical",
                    is_title_bold: true,
                    items: widgetItems,
                },
            };
        }
    } else if (widgetType === "widget_autoplay" && widgetChildType === "widget_child_autoplay") {
        const itemsSql = carousel.secondary_data;
        const itemsData = await getSecondaryData(itemsSql);

        if (itemsData.length > 0) {
            const widgetItems = await Promise.all(itemsData.map(async (eachItem) => {
                const it = { ...eachItem };
                // it.video_resource = {
                //     resource: it.video_resource,
                //     auto_play_duration: it.auto_play_duration,
                // };
                it.id = parseInt(it.id);
                it.is_live = Boolean(it.is_live);
                it.is_vip = Boolean(it.is_vip);
                it.is_premium = Boolean(it.is_premium);
                it.show_reminder = Boolean(it.show_reminder);
                it.state = 2;
                it.live_at = new Date(it.live_at).valueOf();
                it.set_width = true;

                delete it.auto_play_duration;
                delete it.top_title;
                it.color = "#00000000";
                it.default_mute = Boolean(it.default_mute);
                it.auto_play = Boolean(it.auto_play);
                // const videoResources = await generateVideoResourceObject(db, config, data[i].answer_id, obj.id, ['DASH', 'HLS', 'RTMP', 'BLOB', 'YOUTUBE'], versionCode, false);
                it.image_bg_card = buildStaticCdnUrl(LiveclassData.getBgImageForLiveCarousel(it.subject.toLowerCase()));
                // it.subject = 'sf';
                it.card_width = it.card_width || "1.1";
                it.card_ratio = it.card_ratio || "12:8";
                it.text_color_primary = "#ffffff";
                it.text_color_secondary = "#ffffff";
                it.text_color_title = "#ffffff";
                it.set_width = true;
                it.button_state = "multiple";
                it.image_vertical_bias = 1;
                it.bg_exam_tag = "#b02727";
                it.text_color_exam_tag = "#ffffff";
                it.target_exam = "";
                const vidSql = `select * from answer_video_resources where answer_id = ${it.answer_id}  and is_active=1 order by resource_order asc`;
                const videoResources = await mysql.pool.query(vidSql).then((res) => res[0]);
                // config, item, timeout, questionID, versionCode, offsetEnabled);

                if (videoResources && videoResources.length) {
                    const returned = generateVideoResourceObject(videoResources[0], 4, it.id, -1, false);
                    // console.log(videoResources[0],returned);
                    it.video_resource = {
                        resource: returned.resource,
                        drm_scheme: returned.drm_scheme,
                        cdn_base_url: "https://d3cvwyf9ksu0h5.cloudfront.net/",
                        media_type: returned.media_type,
                        drm_license_url: returned.drm_license_url,
                        fallback_url: "https://d3cvwyf9ksu0h5.cloudfront.net/text",
                        hls_timeout: 0,
                        auto_play_initiation: 500,
                        auto_play_duration: 15000,
                    };
                    it.bottom_layout = {
                        title: it.bottom_title,
                        title_color: "#000000",
                        sub_title: it.sub_title,
                        sub_title_color: "#000000",
                        button: {
                            text: it.bottom_title,
                            text_color: "#ea532c",
                            background_color: "#ffffff",
                            border_color: "#ea532c",
                            action: {
                                action_data: {},
                            },
                        },
                    };
                    return {
                        type: "widget_child_autoplay",
                        data: it,
                    };
                }
            }));

            androidWidget = {
                widget_type: "widget_autoplay",
                layout_config: {
                    margin_top: 16,
                    bg_color: "#ffffff",
                },
                order: carousel.caraousel_order,
                widget_data: {
                    carousel_id: carousel.id,
                    title: carousel.title,
                    // "is_live": true,
                    live_text: "LIVE",
                    auto_play: true,
                    full_width_cards: true,
                    auto_play_initiation: 500,
                    auto_play_duration: 15000,
                    scroll_direction: "horizontal",
                    items: widgetItems,
                },
            };
        }
    } else if (widgetType === "widget_parent" && widgetChildType === "auto_scroll_home_banner") {
        const itemsSql = carousel.secondary_data;
        const itemsData = await getSecondaryData(itemsSql);

        if (itemsData.length > 0) {
            const widgetItems = itemsData.map((eachItem) => {
                const it = { ...eachItem };
                delete it.autoplay_duration;
                const widgetItem = {
                    type: "auto_scroll_home_banner",
                    data: it,
                };
                return widgetItem;
            });

            androidWidget = {
                widget_type: "widget_parent",
                widget_data: {
                    title: carousel.title,
                    is_title_bold: true,
                    autoplay_duration: itemsData[0].autoplay_duration,
                    remove_padding: true,
                    scroll_direction: "horizontal",
                    items: widgetItems,
                },
                layout_config: {
                    margin_top: 5,
                    margin_bottom: 5,
                    margin_left: 0,
                    margin_right: 0,
                },
            };
        }
    } else if (widgetType === "widget_parent_tab" && widgetChildType === "widget_ncert_book") {
        const itemsSql = carousel.secondary_data;
        const itemsData = await getSecondaryData(itemsSql);

        if (itemsData.length > 0) {
            const widgetItems = itemsData.map((eachItem) => {
                const it = { ...eachItem };
                it.id = parseInt(it.id);
                it.type = "book";
                it.page = "LIBRARY";
                it.open_new_page = true;
                it.card_width = it.card_width || "2.6";
                it.card_ratio = it.card_ratio || "1:1";
                it.image_corner_radius = 8;
                it.image_url = it.thumbnail_url;
                it.bottom_title = it.display_name;
                it.background_color = it.background_color || "#E2EDDF";
                it.bottom_subtitle = it.bottom_subtitle || "";
                it.bottom_subtitle_color = it.bottom_subtitle_color || "#54138a";
                it.bottom_subtitle_icon = it.bottom_subtitle_icon || "";
                it.deeplink = it.app_deeplink;

                it.layout_padding = {
                    padding_start: 0,
                    padding_end: 0,
                    padding_top: 15,
                    padding_bottom: 20,
                };
                return it;
            });
            const groupedData = _.groupBy(widgetItems, "title");

            const tabs = [];
            for (const key in groupedData) {
                if (groupedData[key]) {
                    tabs.push({
                        key: `${key}`,
                        title: groupedData[key][0].title,
                        is_selected: false,
                    });
                    const widgeted = groupedData[key].map((eit) => ({
                        type: "widget_ncert_book",
                        data: eit,
                        layout_config: {
                            margin_top: 0,
                            margin_bottom: 0,
                            margin_left: 12,
                            margin_right: 4,
                        },
                    }));
                    groupedData[key] = widgeted;
                }
            }

            if (tabs.length) {
                tabs[0].is_selected = true;
            }

            androidWidget = {
                widget_type: "widget_parent_tab",
                layout_config: {
                    margin_top: 0,
                    margin_bottom: 14,
                    margin_left: 0,
                    margin_right: 0,
                },
                layout_padding: {
                    padding_start: 16,
                    padding_end: 16,
                },
                order: carousel.caraousel_order,
                widget_data: {
                    title: carousel.title,
                    original_title: carousel.title,
                    title_text_size: 16,
                    is_title_bold: true,
                    tabs_background_color: itemsData[0].tabs_background_color || "#E2EDDF",
                    background_color: itemsData[0].overall_background_color || "#E2EDDF",
                    items: groupedData,
                    tabs,
                    carousel_id: carousel.id,
                    scroll_direction: carousel.scroll_type,
                },
            };
            if (carousel.scroll_type == "vertical") {
                androidWidget.widget_data.link_text = carousel.locale == "hi" ? "सभी देखें" : "View All";
                androidWidget.widget_data.deeplink = `doubtnutapp://paginated_bottom_sheet_widget?id=homepage_vertical&tab_id=${carousel.id}`;
            }
        }
    }
    return androidWidget;
}

async function getVideoWidget(carousel) {
    let itemsSql = "";
    if (!carousel.secondary_data) {
        return null;
    }
    if (carousel.secondary_data.includes("limit")) {
        itemsSql = carousel.secondary_data.split("limit")[0];
    } else if (carousel.secondary_data.includes("LIMIT")) {
        itemsSql = carousel.secondary_data.split("LIMIT")[0];
    } else if (carousel.secondary_data.includes("Limit")) {
        itemsSql = carousel.secondary_data.split("Limit")[0];
    }
    if (!itemsSql) {
        return null;
    }
    let androidWidget = {};

    itemsSql += " limit 20";
    const [widgetType, widgetChildType] = carousel.data_type.split("#!#");
    if (widgetType === "widget_parent" && widgetChildType === "live_class_carousel_card_2") {
        const itemsData = await getSecondaryData(itemsSql);

        if (itemsData.length > 0) {
            const thumbnailData = await qidThumbnailCheck(itemsData);
            const widgetItems = itemsData.map((eachItem) => {
                const it = { ...eachItem };
                it.button = {
                    text: it["GO TO CHAPTER"],
                    deeplink: it.button_deeplink,
                };
                it.is_live = Boolean(it.is_live);
                it.is_vip = Boolean(it.is_vip);
                it.is_premium = Boolean(it.is_premium);
                it.show_reminder = Boolean(it.show_reminder);
                it.state = 2;
                it.live_at = new Date(it.live_at).valueOf();
                it.set_width = true;

                if (it.thumbnail) {
                    it.image_bg_card = !thumbnailData[it.id] ? `${config.staticCDN}q-thumbnail/${it.id}.webp` : it.thumbnail;
                    it.subject = "";
                    it.title = "";
                    it.title1 = "";
                    it.title2 = "";
                    it.image_url = "";
                } else {
                    it.image_bg_card = buildStaticCdnUrl(LiveclassData.getBgImage(it.subject.toLowerCase()));
                }
                // it.image_bg_card = it.thumbnail || buildStaticCdnUrl(LiveclassData.getBgImage(it.subject.toLowerCase()));
                it.color = getBarColorForRecentclassHomepage(it.subject);
                it.lock_state = 0;
                it.topic = "";
                it.students = 13822;
                it.interested = 13822;
                it.card_width = it.card_width || "1.1";
                it.card_ratio = it.card_ratio || "16:9";
                const textColors = getTextColor(it.subject.toLowerCase());
                it.text_color_primary = textColors.text_color_primary || "#ffffff";
                it.text_color_secondary = textColors.text_color_secondary || "#ffffff";
                it.text_color_title = textColors.text_color_title || "#ffffff";
                return {
                    type: "live_class_carousel_card_2",
                    data: it,
                };
            });

            androidWidget = {
                carousel_id: carousel.id,
                widget_type: "widget_parent",
                layout_config: {
                    margin_top: 16,
                    bg_color: "#FFFFFF",
                },
                order: carousel.caraousel_order,
                widget_data: {
                    title: carousel.title,
                    is_title_bold: true,
                    items: widgetItems,
                    carousel_id: carousel.id,
                    scroll_direction: carousel.scroll_type,
                },
            };
        }
    } else if (widgetType === "widget_parent_tab" && widgetChildType === "live_class_carousel_card_2") {
        const itemsData = await getSecondaryData(itemsSql);

        if (itemsData.length > 0) {
            const thumbnailData = await qidThumbnailCheck(itemsData);
            const widgetItems = itemsData.map((eachItem) => {
                const it = { ...eachItem };
                it.button = {
                    text: it.button_text,
                    deeplink: it.button_deeplink,
                };
                it.id = parseInt(it.id);
                it.is_live = Boolean(it.is_live);
                it.is_vip = Boolean(it.is_vip);
                it.is_premium = Boolean(it.is_premium);
                it.show_reminder = Boolean(it.show_reminder);
                it.state = 2;
                it.live_at = new Date(it.live_at).valueOf();
                it.set_width = true;

                if (it.thumbnail) {
                    it.image_bg_card = !thumbnailData[it.id] ? `${config.staticCDN}q-thumbnail/${it.id}.webp` : it.thumbnail;
                    it.subject = "";
                    it.title1 = "";
                    it.title2 = "";
                    it.image_url = "";
                } else {
                    it.image_bg_card = buildStaticCdnUrl(LiveclassData.getBgImage(it.subject.toLowerCase()));
                }
                it.color = getBarColorForRecentclassHomepage(it.subject);
                it.lock_state = 0;
                it.set_width = true;
                it.topic = "";
                it.students = 13822;
                it.interested = 13822;
                it.card_width = it.card_width || "1.1";
                it.card_ratio = it.card_ratio || "16:9";
                const textColors = getTextColor(it.subject.toLowerCase());
                it.text_color_primary = textColors.text_color_primary || "#ffffff";
                it.text_color_secondary = textColors.text_color_secondary || "#ffffff";
                it.text_color_title = textColors.text_color_title || "#ffffff";
                return it;
            });
            androidWidget = {
                carousel_id: carousel.id,
                widget_type: "widget_parent",
                layout_config: {
                    margin_top: 16,
                    bg_color: "#FFFFFF",
                },
                order: carousel.caraousel_order,
                widget_data: {
                    title: carousel.title,
                    is_title_bold: true,
                    items: widgetItems,
                    carousel_id: carousel.id,
                    scroll_direction: carousel.scroll_type,
                },
            };
        }
    }
    if (Object.keys(androidWidget).length) {
        return androidWidget;
    }

    return null;
}

const RealTimeTypes = [
    "DPP", "HOMEWORK", "PLAYLIST", "CHANNELS", "HISTORY", "HOMEPAGE_GAMES_LIST", "VIDEO_ADS", "DOUBTFEED", "PROFILE_DETAILS", "BANNER", "KHELO_JEETO", "PREVIOUS_YEAR_SOLUTIONS",
    "COURSES", "VIDEO_GEN", "CTET_CAROUSELS", "DEFENCE_CAROUSELS", "LIVECLASS_GEN", "TOP_FREE_CLASSES", "PENDING_PAYMENT", "ETOOS_FACULTY", "ETOOS_FREE_VIDEOS", "ETOOS_FACULTY_FREE_VIDEOS",
    "MY_COURSES", "MY_SACHET", "DICTIONARY_BANNER", "CTET_BANNER", "TRENDING_EXAM", "STUDY_TIMER_BANNER", "PRACTICE_ENGLISH_BANNER", "DNR_BANNER", "TRENDING_BOARD", "REVISION_CLASSES",
    "JEE_MAINS_ADV", "NKC_SIR_VIDEOS", "PREVIOUS_YEARS_PDF", "SAMPLE_PAPERS_PDF", "PREVIOUS_YEARS_PDF_EXAM_WISE", "DAILY_QUIZ", "SFY", "NUDGE", "WIDGET_NUDGE", "CALLING_CARD", "EXAM_CORNER",
    "CATEGORY_ICONS", "LATEST_COURSES", "SUBSCRIBED_TEACHERS", "CCM_TEACHERS", "LIBRARY_TRENDING", "SUBSCRIBED_TEACHERS_VIDEOS", "COURSE_TRIAL_TIMER", "TOP_TEACHERS_CLASSES", "RECOMMENDED_INTERNAL_TEACHERS",
    "SUBSCRIBED_INTERNAL_TEACHERS", "SUBSCRIBED_INTERNAL_TEACHERS_VIDEOS", "PRACTICE_ENGLISH_WIDGET", "SPOKEN_ENGLISH_CERTIFICATE", "MOCK_TEST_WIDGET", "REFEREE_NUDGE", "ADV_BANNER", "RECOMMENDED_VIDEO",
    "NEW_CLP_CATEGORIES", "AD_HOC_WIDGETS_AM", "LIVECLASS_V1", "COURSES_V1", "REFERRAL_V2",
];

async function carouselCreator(carousel, studentClass, limit, description, button, duration, actionActivity, subjectUrl, baseUrl, weekNo, language, type) {
    carousel.description = description;
    if (carousel.data_type === "topic") {
        const sql = `select id,'${carousel.data_type}' as type, subject, image_url, name as title, '' as description, is_last, resource_type, playlist_order, master_parent from new_library where parent = '${carousel.mapped_playlist_id} ' and is_active = 1 and is_delete = 0 order by rand() LIMIT 5`;
        let data = await mysql.pool.query(sql).then((res) => res[0]);
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: null,
                show_whatsapp: showWhatsapp(carousel),
                show_video: false,
                image_url: elem.image_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: encodeURI(`doubtnutapp://playlist?playlist_id=${elem.id}&playlist_title=${elem.title}&is_last=${elem.is_last}`),
                id: elem.id,
            };
            return mappedObject;
        });

        carousel.items = data;
        carousel.deeplink = encodeURI(`doubtnutapp://playlist?playlist_id=${carousel.mapped_playlist_id}&playlist_title=${carousel.title}&is_last=0`);
        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.data_type === "topic_parent") {
        if (carousel.type === "PERSONALIZATION_CHAPTER" || carousel.type === "PERSONALIZATION_LIVE_CLASS" || carousel.type === "PERSONALIZATION_BOOKS" || carousel.type === "PERSONALIZATION_NCERT_TAB" || carousel.type === "GOVT_BOOKS") {
            carousel.carousel_type = "real_time";
            return carousel;
        }
        const sql = `select id, subject, image_url, name as title, '' as description, is_last, resource_type, playlist_order, master_parent, case when resource_type = 'web_view' then resource_path else '' end as resource_path from new_library where parent in ('${carousel.mapped_playlist_id}') and is_active = 1 and is_delete = 0 order by rand() LIMIT ${limit}`;
        let data = await mysql.pool.query(sql).then((res) => res[0]);

        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: showWhatsapp(carousel),
                show_video: false,
                image_url: elem.image_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: (elem.resource_type == "web_view") ? encodeURI(`doubtnutapp://web_view?chrome_custom_tab=true&url=${elem.resource_path}`) : encodeURI(`doubtnutapp://playlist?playlist_id=${elem.id}&playlist_title=${elem.title}&is_last=${elem.is_last}`),
                id: elem.id,
            };
            return mappedObject;
        });
        carousel.deeplink = encodeURI(`doubtnutapp://playlist?playlist_id=${carousel.mapped_playlist_id}&playlist_title=${carousel.title}&is_last=0`);
        carousel.items = data;
        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "WHATSAPP_ASK") {
        const sql = "select * from app_configuration where class in ('all',?) and is_active=1 and key_name='whatsapp_ask'";
        const data = await mysql.pool.query(sql, [studentClass]).then((res) => res[0]);
        if (data) {
            const keyValue = JSON.parse(data[0].key_value);
            carousel.title = keyValue.description;
            carousel.deeplink = encodeURI(`doubtnutapp://external_url?url=${keyValue.action_data.url}`);
            carousel.image_url = keyValue.image_url;
        }

        carousel.show_view_all = 0;
        carousel.items = [];

        carousel.widget_type = "whatsapp";
        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "CRASH_COURSE") {
        // NO ACTIVE Carousels

        // const sql = `SELECT cast(c.question_id as char(50)) AS id, '${type}' as type, c.subject, case when c.matched_question is null then concat('${baseUrl}', c.question_id, '.png') else concat('${baseUrl}', c.matched_question, '.png') end as image_url, left(c.ocr_text, 100) AS title, left('${description}', 100) as description, c.resource_type FROM(select id from student_playlists where name like '%CRASH COURSE%' and student_id = '98') as a inner join(select question_id, playlist_id from playlist_questions_mapping) as b on a.id = b.playlist_id inner join(select question_id, matched_question, ocr_text, subject,case when is_text_answered = 1 and is_answered = 0 then 'text' else 'video' end as resource_type from questions WHERE is_answered = 1 or is_text_answered = 1) as c on c.question_id = b.question_id left join(select chapter, question_id from questions_meta) as d on b.question_id = d.question_id left join(select GROUP_CONCAT(packages) as packages, question_id from question_package_mapping group by question_id) as e on e.question_id = b.question_id left join(select duration, question_id from answers order by answer_id DESC limit 1) as f on f.question_id = c.question_id order by rand() limit ${limit}`;
        // let data = await mysql.pool.query(sql).then((res) => res[0]);

        // data = data.map((elem) => {
        //     elem.deeplink = `doubtnutapp://video?qid=${elem.id}&page=HOME_FEED&playlist_id=${carousel.mapped_playlist_id}`
        //     return elem
        // })
        carousel.items = [];

        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "LATEST_FROM_DOUBTNUT") {
        const sql = `SELECT cast(a.question_id as char(50)) AS id, case when b.matched_question is null then concat('${baseUrl}',a.question_id,'.png') else concat('${baseUrl}',b.matched_question,'.png') end as image_url, left(b.ocr_text,100) AS title,left('${carousel.description}',100) as description,b.resource_type FROM (select question_id,id from engagement where type='viral_videos' and class in ('${studentClass}','all') and start_date <= CURRENT_TIMESTAMP  and end_date >= CURRENT_TIMESTAMP) as a inner join (select question_id,question,matched_question,ocr_text,chapter,student_id,case when is_text_answered=1 and is_answered=0 then 'text' else 'video' end as resource_type from questions where student_id in ('80','82','83','85','86','87','88','89','90','98') and (is_answered=1 or is_text_answered=1) and is_skipped=0) as b on a.question_id=b.question_id left join (select duration,question_id from answers order by answer_id DESC limit 1) as d on d.question_id=a.question_id left join (select * from studentid_package_mapping) as z on z.student_id=b.student_id order by a.id desc LIMIT ${limit}`;
        let data = await mysql.pool.query(sql).then((res) => res[0]);
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: null,
                show_whatsapp: true,
                show_video: true,
                image_url: elem.image_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: encodeURI(`doubtnutapp://video?qid=${elem.id}&page=HOME_FEED&playlist_id=${carousel.type}`),
                id: elem.id,
            };
            return mappedObject;
        });
        carousel.deeplink = encodeURI(`doubtnutapp://playlist?playlist_id=${carousel.mapped_playlist_id}&playlist_title=${carousel.title}&is_last=1`);
        carousel.items = data;
        carousel.carousel_type = "carousel";

        return carousel;
    } if (carousel.data_type === "library_video") {
        let data = [];
        if (carousel.type === "VMC") {
            if (carousel.ccm_id === 11101 || carousel.ccm_id === 11201 || carousel.ccm_id === 11301) {
                const stClass = (carousel.class === 13) ? 12 : carousel.class;
                // const sql = `SELECT  d.topic, e.question_id, SUM(rating) as score from (select  c.topic, c.resource_reference from ((select * from liveclass_course where title like '%JEE%' and class in ('${stClass}') and id in (10, 11, 12, 13, 14, 19)) as a left join (select * from liveclass_course_details where live_at < now()) as b on a.id=b.liveclass_course_id left join (SELECT * FROM liveclass_course_resources WHERE resource_type in (1,8)) as c on b.id=c.liveclass_course_detail_id)) as d left join user_answer_feedback as e on d.resource_reference=e.question_id group by e.question_id order by score DESC LIMIT 5 `;
                const sql = `SELECT  d.topic, e.question_id, SUM(rating) as score from (select  c.topic, c.primary_key from ((select * from liveclass_course where title like '%JEE%' and  class in ('${stClass}') and id in (10, 11, 12, 13, 14, 19)) as a left join (select * from liveclass_course_details where live_at < now()) as b on a.id=b.liveclass_course_id left join (SELECT *, CASE WHEN resource_type=1 AND PLAYER_TYPE='youtube' THEN meta_info ELSE resource_reference END as primary_key FROM liveclass_course_resources WHERE resource_type in (1,8)) as c on b.id=c.liveclass_course_detail_id)) as d left join user_answer_feedback as e on d.primary_key=e.question_id group by e.question_id order by score DESC LIMIT 5`;
                data = await mysql.pool.query(sql).then((res) => res[0]);
            } else if (carousel.ccm_id === 11103 || carousel.ccm_id === 11203 || carousel.ccm_id === 11303) {
                const stClass = (carousel.class === 13) ? 12 : carousel.class;
                // const sql = `SELECT  d.topic, e.question_id, SUM(rating) as score from (select  c.topic, c.resource_reference from ((select * from liveclass_course where title like '%NEET%' and class in ('${stClass}') and id in (10, 11, 12, 13, 14, 19)) as a left join (select * from liveclass_course_details where live_at < now()) as b on a.id=b.liveclass_course_id left join (SELECT * FROM liveclass_course_resources WHERE resource_type in (1,8)) as c on b.id=c.liveclass_course_detail_id)) as d left join user_answer_feedback as e on d.resource_reference=e.question_id group by e.question_id order by score DESC LIMIT 5 `;
                const sql = `SELECT  d.topic, e.question_id, SUM(rating) as score from (select  c.topic, c.primary_key from ((select * from liveclass_course where title like '%NEET%' and  class in ('${stClass}') and id in (10, 11, 12, 13, 14, 19)) as a left join (select * from liveclass_course_details where live_at < now()) as b on a.id=b.liveclass_course_id left join (SELECT *, CASE WHEN resource_type=1 AND PLAYER_TYPE='youtube' THEN meta_info ELSE resource_reference END as primary_key FROM liveclass_course_resources WHERE resource_type in (1,8)) as c on b.id=c.liveclass_course_detail_id)) as d left join user_answer_feedback as e on d.primary_key=e.question_id group by e.question_id order by score DESC LIMIT 5`;
                // console.log(sql);
                data = await mysql.pool.query(sql).then((res) => res[0]);
            } else {
                const stClass = (carousel.class === 13) ? 12 : carousel.class;
                // const sql = `SELECT  d.topic, e.question_id, SUM(rating) as score from (select  c.topic, c.resource_reference from ((select * from liveclass_course where class in ('${stClass}') and id in (10, 11, 12, 13, 14, 19)) as a left join (select * from liveclass_course_details where live_at < now()) as b on a.id=b.liveclass_course_id left join (SELECT * FROM liveclass_course_resources WHERE resource_type in (1,8)) as c on b.id=c.liveclass_course_detail_id)) as d left join user_answer_feedback as e on d.resource_reference=e.question_id group by e.question_id order by score DESC LIMIT 5 `;
                const sql = `SELECT  d.topic, e.question_id, SUM(rating) as score from (select  c.topic, c.primary_key from ((select * from liveclass_course where class in ('${stClass}') and id in (10, 11, 12, 13, 14, 19)) as a left join (select * from liveclass_course_details where live_at < now()) as b on a.id=b.liveclass_course_id left join (SELECT *, CASE WHEN resource_type=1 AND PLAYER_TYPE='youtube' THEN meta_info ELSE resource_reference END as primary_key FROM liveclass_course_resources WHERE resource_type in (1,8)) as c on b.id=c.liveclass_course_detail_id)) as d left join user_answer_feedback as e on d.primary_key=e.question_id group by e.question_id order by score DESC LIMIT 5`;
                // console.log(sql);
                data = await mysql.pool.query(sql).then((res) => res[0]);
            }
            data = data.map((elem) => {
                const mappedObject = {
                    title: elem.topic,
                    subtitle: null,
                    show_whatsapp: true,
                    show_video: true,
                    image_url: `http://d10lpgp6xz60nq.cloudfront.net/q-thumbnail/${elem.question_id}.png`,
                    card_width: carousel.scroll_size, // percentage of screen width
                    aspect_ratio: "",
                    deeplink: encodeURI(`doubtnutapp://video?qid=${elem.question_id}&page=HOME`),
                    id: elem.question_id,
                };
                return mappedObject;
            });
            carousel.deeplink = encodeURI("doubtnutapp://library_tab");
            carousel.items = data;
            carousel.carousel_type = "carousel";
            // console.log(carousel);
            return carousel;
        }
        // console.log("wrong");
        carousel.carousel_type = "real_time";
        return carousel;
    } if (carousel.data_type === "liveclass") {
        carousel.carousel_type = "real_time";
        return carousel;
    } if (carousel.type === "TRICKY_QUESTION") {
        const sql = `select cast(a.question_id as char(50)) AS id,'${carousel.type}' as type,'video_wrapper' as layout_type,case when a.matched_question is null then concat('${baseUrl}',a.question_id,'.png') else concat('${baseUrl}',a.matched_question,'.png') end as image_url,left(a.ocr_text,100) AS title,left('${carousel.description}',100) as description,'video' as resource_type,a.subject,case when a.subject='MATHS' then '${subjectUrl[2]}' when a.subject='PHYSICS' then '${subjectUrl[0]}' when a.subject='CHEMISTRY' then '${subjectUrl[1]}' when a.subject='BIOLOGY' then '${subjectUrl[3]}' else '${subjectUrl[4]}' end as subject_image,CASE when SUBSTRING_INDEX(f.book_meta,'||',2)='Physics || Books' then SUBSTR(f.book_meta,21) when SUBSTRING_INDEX(f.book_meta,'||',2)='Chemistry || Books' then SUBSTR(f.book_meta,23) when SUBSTRING_INDEX(f.book_meta,'||',2)='Maths || Books' then SUBSTR(f.book_meta,18) when SUBSTRING_INDEX(f.book_meta,'||',2)='Biology || Books' then SUBSTR(f.book_meta,21) when SUBSTRING_INDEX(f.book_meta,'||',1)='Biology' then SUBSTR(f.book_meta,12) when SUBSTRING_INDEX(f.book_meta,'||',1)='Maths' then SUBSTR(f.book_meta,10) when SUBSTRING_INDEX(f.book_meta,'||',1)='Physics' then SUBSTR(f.book_meta,12) when SUBSTRING_INDEX(f.book_meta,'||',1)='Chemistry' then SUBSTR(f.book_meta,14) else f.book_meta end as book_meta FROM (SELECT question_id,matched_question,ocr_text,subject from questions WHERE student_id= 100 and class='${studentClass}' and RIGHT(LEFT(doubt,9),2)='${weekNo}' and is_answered=1) as a left join (select question_id, max(answer_id) as answer_id from answers group by question_id) as d on d.question_id=a.question_id left join answers as e on d.answer_id = e.answer_id inner join book_meta as f on a.matched_question=f.question_id ORDER by rand() LIMIT ${limit}`;
        // console.log(sql);
        if (!TrickyQuestionsCache[studentClass]) {
            TrickyQuestionsCache[studentClass] = await mysql.pool.query(sql).then((res) => res[0]);
        }
        // console.log(TrickyQuestionsCache[studentClass]);

        const data = TrickyQuestionsCache[studentClass].map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: true,
                show_video: true,
                image_url: elem.image_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: encodeURI(`doubtnutapp://video?qid=${elem.id}&page=HOME_FEED&playlist_id=${carousel.type}`),
                id: elem.id,
            };
            return mappedObject;
        });
        carousel.show_view_all = 0;
        carousel.items = data;
        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "CONCEPT_BOOSTER") {
        // No active carousel

        // const student_course = "HOME_PAGE_CC";
        // const sql = `select concat(a.class,a.course,a.chapter_order) as id,CONVERT(a.class,CHAR(50)) as class, a.course as course,'${type}' as type,c.path_image as image_url,a.chapter as title,'' as description, a.chapter, 'CONCEPT BOOSTER' as capsule_text from (select DISTINCT chapter,chapter_order, class,course from mc_course_mapping where class = ${studentClass} AND course = '${student_course}' AND active_status > 0 order by class ASC, chapter_order ASC) as a left join (select * from class_chapter_image_mapping where class = ${studentClass} AND course = '${student_course}') as c on a.chapter = c.chapter and a.class=c.class and a.course=c.course order by rand() limit ${limit}`;
        // let data = await mysql.pool.query(sql).then((res) => res[0]);
        // data = data.map((elem) => {
        //     elem.deeplink = `doubtnutapp://playlist?playlist_id=${elem.id}&playlist_title=${elem.title}&is_last=${elem.is_last}`
        //     return elem
        // })
        carousel.items = [];
        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "TRENDING") {
        const sql = `SELECT cast(a.question_id as char(50)) AS id,'${carousel.type}' as type, case when b.matched_question is null then concat('${baseUrl}',a.question_id,'.png') else concat('${baseUrl}',b.matched_question,'.png') end as image_url, left(b.ocr_text,100) AS title,left('${carousel.description}',100) as description,b.resource_type FROM (select question_id,created_at from trending_videos where date(created_at) >= DATE_SUB(CURDATE(), INTERVAL 2 DAY) AND class = '${studentClass}' LIMIT 5) as a left join (select question_id,ocr_text,question,matched_question,case when is_text_answered=1 and is_answered=0 then 'text' else 'video' end as resource_type from questions) as b on a.question_id = b.question_id left join (select c.chapter,d.packages, c.question_id from questions_meta as c left join (select GROUP_CONCAT(packages) as packages,question_id from question_package_mapping group by question_id) as d on c.question_id=d.question_id ) as e on a.question_id=e.question_id left join(select duration,question_id from answers order by answer_id DESC limit 1 ) as f on f.question_id=a.question_id order by a.created_at desc LIMIT ${limit}`;
        let data = await mysql.pool.query(sql).then((res) => res[0]);
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: true,
                show_video: true,
                image_url: elem.image_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: `doubtnutapp://video?qid=${elem.id}&page=HOME_FEED&playlist_id=${carousel.type}`,
                id: elem.id,
            };
            return mappedObject;
        });
        carousel.items = data;
        carousel.deeplink = encodeURI(`doubtnutapp://playlist?playlist_id=${carousel.mapped_playlist_id}&playlist_title=${carousel.title}&is_last=1`);

        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "VIRAL") {
        const sql = `SELECT cast(a.question_id as char(50)) AS id, case when a.matched_question is null then concat('${baseUrl}', a.question_id, '.png') else concat('${baseUrl}', a.matched_question, '.png') end as image_url, left(a.ocr_text, 100) AS title, left('${carousel.description}', 100) as description, a.resource_type FROM(select matched_question, question_id, student_id, ocr_text,case when is_text_answered = 1 and is_answered = 0 then 'text' else 'video' end as resource_type from questions WHERE student_id = 81) as a left join(select chapter, question_id from questions_meta) as b on a.question_id = b.question_id left join(select duration, question_id from answers order by answer_id DESC limit 1) as d on d.question_id = b.question_id left join(select * from studentid_package_mapping) as z on z.student_id = a.student_id order by rand() LIMIT ${limit} `;
        // console.log(sql);
        let data = await mysql.pool.query(sql).then((res) => res[0]);
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: true,
                show_video: true,
                image_url: elem.image_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: `doubtnutapp://video?qid=${elem.id}&page=HOME_FEED&playlist_id=${carousel.type}`,
                id: elem.id,
            };
            return mappedObject;
        });
        carousel.items = data;
        carousel.deeplink = encodeURI(`doubtnutapp://playlist?playlist_id=${carousel.mapped_playlist_id}&playlist_title=${carousel.title}&is_last=1`);
        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "NCERT_SOLUTIONS" && carousel.data_type === "ncert") {
        const sql = `select a.id as playlist_id,a.name as id,'${carousel.type}' as type,a.image_url as image_url, case when b.${language} is null then a.name else b.${language} end as title,'${studentClass}' as class,left('${carousel.description}',40) as description, 'NCERT' as capsule_text from (select * from new_library where parent='${carousel.mapped_playlist_id}' and is_active=1 and is_delete=0 order by id asc ) as a left join (select DISTINCT chapter,${language} from localized_ncert_chapter where class='${studentClass}') as b on a.name=b.chapter order by rand() LIMIT ${limit}`;
        let data = await mysql.pool.query(sql).then((res) => res[0]);
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: true,
                show_video: true,
                image_url: elem.image_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: `doubtnutapp://video?qid=${elem.id}&page=HOME_FEED&playlist_id=${carousel.mapped_playlist_id}`,
                id: elem.id,
            };
            return mappedObject;
        });
        carousel.items = data;
        carousel.deeplink = encodeURI(`doubtnutapp://playlist?playlist_id=${carousel.mapped_playlist_id}&playlist_title=${carousel.title}&is_last=1`);
        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "GK") {
        const sql = `SELECT cast(a.question_id as char(50)) AS id, '${type}' as type, case when a.matched_question is null then concat('${baseUrl}', a.question_id, '.png') else concat('${baseUrl}', a.matched_question, '.png') end as image_url, left(a.ocr_text, 50) AS title, left('${carousel.description}', 40) as description, b.chapter, c.packages as capsule_text, d.duration, '${duration[0]}' as duration_text_color, '${duration[1]}' as duration_bg_color FROM(select question_id, matched_question, ocr_text, student_id from questions WHERE student_id = 82) as a left join(select chapter, question_id from questions_meta) as b on a.question_id = b.question_id left join(select GROUP_CONCAT(packages) as packages, question_id from question_package_mapping group by question_id) as c on a.question_id = c.question_id left join(select duration, question_id from answers order by answer_id DESC) as d on d.question_id = c.question_id LIMIT ${limit} `;
        let data = await mysql.pool.query(sql).then((res) => res[0]);
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: true,
                show_video: true,
                image_url: elem.image_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: `doubtnutapp://video?qid=${elem.id}&page=HOME_FEED&playlist_id=${carousel.mapped_playlist_id}`,
                id: elem.id,
            };
            return mappedObject;
        });
        carousel.show_view_all = 0;
        carousel.items = data;
        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "DAILY_CONTEST") {
        const sql = `select cast(id as char(50)) as id,contest_name as title, logo as image_url, headline as description, '${button[0]}' as button_text,'${button[2]}' as button_bg_color,'${button[1]}' as button_text_color from contest_details where date_from<=curdate() && date_till>=curdate() LIMIT ${limit}`;
        let data = await mysql.pool.query(sql).then((res) => res[0]);
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: false,
                show_video: false,
                image_url: elem.image_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: `doubtnutapp://daily_contest_with_contest_id?contest_id=${elem.id}`,
                id: elem.id,
            };
            return mappedObject;
        });

        carousel.items = data;
        carousel.show_view_all = 1;
        carousel.deeplink = "doubtnutapp://daily_contest";
        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "APP_BANNER") {
        // let subtitle = ''
        // carousel.scroll_type = 'APP_BANNER'
        // if (carousel.scroll_size === '1x') {
        //     let sql = "select cast(id as char(50)) as id, image_url, left(description,50) as title," +
        //         " actionActivity, action_data,'" + button[0] + "' as button_text, '" + button[1] + "' as button_text_color, '" + button[2] + "' as button_bg_color, '" + subtitle + "' as subtitle, '" + description + "' as description from app_banners where type ='1x' and is_active=1 and class in ('" + studentClass + "','all') and page_type like '%HOME%' and min_version_code<585 and max_version_code>=585 order by banner_order limit " + limit;
        //     let data = await mysql.pool.query(sql).then((res) => res[0])
        //     data = data.map((elem) => {
        //         let mappedObject = {
        //             "title": elem.title,
        //             "subtitle": elem.description,
        //             "show_whatsapp": false,
        //             "show_video": false,
        //             "image_url": elem.image_url,
        //             "card_width": carousel.scroll_size, // percentage of screen width
        //             "aspect_ratio": "",
        //             "id": elem.id,
        //         };
        //         elem.action_data = JSON.parse(elem.action_data)
        //         if (elem.actionActivity == 'playlist') {
        //             mappedObject.deeplink = `doubtnutapp://playlist?playlist_id=${elem.action_data.playlist_id}&playlist_title=${encodeURI(elem.action_data.playlist_title)}&is_last=${elem.action_data.is_last}`
        //         }

        //         if (elem.actionActivity == 'camera') {
        //             mappedObject.deeplink = `doubtnutapp://camera`
        //         }
        //         if (elem.actionActivity == 'daily_contest') {
        //             mappedObject.deeplink = `doubtnutapp://daily_contest`
        //         }
        //         if (elem.actionActivity == 'external') {
        //             mappedObject.deeplink = `doubtnutapp://external_url?url=${elem.action_data.url}`
        //         }
        //         return mappedObject
        //     })
        //     carousel.items = data
        //     carousel.carousel_type = 'carousel'
        //     return carousel
        //     // promise.push(AppBannerContainer.getAppBanner1xDataNew(carouselOrder[i].type, carouselOrder[i].scroll_size, carouselOrder[i].data_type, data.button, data.subtitle, data.description, carouselOrder[i].data_limit, carousel.class, data.version_code, db));
        // } else if (carousel.scroll_size === '1.5x') {
        //     let sql = "select cast(id as char(50)) as id, '" + type + "' as type, image_url,left(description,50) as title, actionActivity, action_data,'" + button[0] + "' as button_text, '" + button[1] + "' as button_text_color, '" + button[2] + "' as button_bg_color, '" + subtitle + "' as subtitle, '" + description + "' as description from app_banners where type='1.5x' and is_active=1 and class in ('" + studentClass + "','all') limit " + limit
        //     let data = await mysql.pool.query(sql).then((res) => res[0])
        //     data = data.map((elem) => {
        //         let mappedObject = {
        //             "title": elem.title,
        //             "subtitle": elem.description,
        //             "show_whatsapp": false,
        //             "show_video": false,
        //             "image_url": elem.image_url,
        //             "card_width": carousel.scroll_size, // percentage of screen width
        //             "aspect_ratio": "",
        //             "id": elem.id,
        //         };
        //         elem.action_data = JSON.parse(elem.action_data)
        //         if (elem.actionActivity == 'playlist') {
        //             mappedObject.deeplink = `doubtnutapp://playlist?playlist_id=${elem.action_data.playlist_id}&playlist_title=${encodeURI(elem.action_data.playlist_title)}&is_last=${elem.action_data.is_last}`
        //         }

        //         if (elem.actionActivity == 'camera') {
        //             mappedObject.deeplink = `doubtnutapp://camera`
        //         }
        //         if (elem.actionActivity == 'daily_contest') {
        //             mappedObject.deeplink = `doubtnutapp://daily_contest`
        //         }
        //         if (elem.actionActivity == 'external') {
        //             mappedObject.deeplink = `doubtnutapp://external_url?url=${elem.action_data.url}`
        //         }
        //         return mappedObject
        //     })
        //     carousel.items = data
        //     carousel.carousel_type = 'carousel'
        //     return carousel
        // promise.push(AppBannerContainer.getAppBanner15xData(carouselOrder[i].type, carouselOrder[i].scroll_size, carouselOrder[i].data_type, data.button, data.subtitle, data.description, carouselOrder[i].data_limit, carousel.class, db));
        // } else if (carousel.scroll_size === '2.5x') {
        //     let sql = "select cast(id as char(50)) as id, '" + type + "' as type, image_url,left(description,50) as title, actionActivity, action_data,'" + button[0] + "' as button_text, '" + button[1] + "' as button_text_color, '" + button[2] + "' as button_bg_color, '" + subtitle + "' as subtitle, '" + description + "' as description from app_banners where type='2.5x' and is_active=1 and class in ('" + studentClass + "','all') limit " + limit
        //     let data = await mysql.pool.query(sql).then((res) => res[0])
        //     data = data.map((elem) => {
        //         let mappedObject = {
        //             "title": elem.title,
        //             "subtitle": elem.description,
        //             "show_whatsapp": false,
        //             "show_video": false,
        //             "image_url": elem.image_url,
        //             "card_width": carousel.scroll_size, // percentage of screen width
        //             "aspect_ratio": "",
        //             "id": elem.id,
        //         };
        //         elem.action_data = JSON.parse(elem.action_data)
        //         if (elem.actionActivity == 'playlist') {
        //             mappedObject.deeplink = `doubtnutapp://playlist?playlist_id=${elem.action_data.playlist_id}&playlist_title=${encodeURI(elem.action_data.playlist_title)}&is_last=${elem.action_data.is_last}`
        //         }

        //         if (elem.actionActivity == 'camera') {
        //             mappedObject.deeplink = `doubtnutapp://camera`
        //         }
        //         if (elem.actionActivity == 'daily_contest') {
        //             mappedObject.deeplink = `doubtnutapp://daily_contest`
        //         }
        //         if (elem.actionActivity == 'external') {
        //             mappedObject.deeplink = `doubtnutapp://external_url?url=${elem.action_data.url}`
        //         }
        //         return mappedObject
        //     })
        //     carousel.items = data
        //     carousel.carousel_type = 'carousel'
        //     return carousel
        //     //     promise.push(AppBannerContainer.getAppBanner25xData(carouselOrder[i].type, carouselOrder[i].scroll_size, carouselOrder[i].data_type, data.button, data.subtitle, data.description, carouselOrder[i].data_limit, carousel.class, db));
        // }
        carousel.carousel_type = "real_time";
        return carousel;
    } if (carousel.type === "QUIZ_WINNER") {
        // let sql = "Select cast(student_id as char(50)) as id,'" + carousel.data_type + "' as type,left(student_username,50) as title,case when img_url='' or img_url is null then 'https://d10lpgp6xz60nq.cloudfront.net/engagement_framework/feed_profile.png' else img_url end as image_url from quiz_winners where date_q=date_sub(CURRENT_DATE, INTERVAL " + 1 + " DAY)";
        // let data = await mysql.pool.query(sql).then((res) => res[0]);
        carousel.items = [];
        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "SUPER_SERIES") {
        const sql = `SELECT DISTINCT cast(package_id as char(50)) as id, package_id,package,NULL as level1,NULL as level2,NULL as location,class,NULL as status,package_order,'${carousel.type}' as type, package as title,img_url,'${description}' as description from pdf_download where package_id in (2,4,20) and class ='${studentClass}' and status=1 order by rand()`;
        let data = await mysql.pool.query(sql).then((res) => res[0]);
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: true,
                show_video: false,
                image_url: elem.img_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: `doubtnutapp://downloadpdf_level_one?pdf_package=${elem.package}`,
                id: elem.id,
            };
            return mappedObject;
        });
        carousel.items = data;
        carousel.show_view_all = 1;
        carousel.deeplink = "doubtnutapp://download_pdf";
        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "JEE MAINS 2019 - APRIL") {
        const sql = `SELECT DISTINCT cast(concat(package_id,level1) as char(50)) as id, package_id,package,level1,NULL as level2,NULL as location,class,NULL as status,package_order,'${carousel.type}' as type, level1 as title,img_url,'${description}' as description from pdf_download where package_id in (28) and class ='${studentClass}' and status=1 order by rand()`;
        let data = await mysql.pool.query(sql).then((res) => res[0]);
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: false,
                show_video: false,
                image_url: elem.img_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: `doubtnutapp://downloadpdf_level_two?pdf_package=${elem.package}&level_one=${elem.level1}`,
                id: elem.id,
            };
            return mappedObject;
        });
        carousel.items = data;

        carousel.deeplink = `doubtnutapp://downloadpdf_level_one?pdf_package=${carousel.type}`;
        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "NEET 2019 SOLUTIONS") {
        const sql = `SELECT DISTINCT cast(concat(package_id,level1) as char(50)) as id, package_id,package,level1,NULL as level2,location,class,NULL as status,package_order,'${carousel.type}' as type, level1 as title,img_url,'${description}' as description from pdf_download where package_id in (29) and class ='${studentClass}' and status=1 order by rand()`;
        let data = await mysql.pool.query(sql).then((res) => res[0]);
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: false,
                show_video: false,
                image_url: elem.img_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: `doubtnutapp://downloadpdf_level_two?pdf_package=${elem.package}&level_one=${elem.level1}`,
                id: elem.id,
            };
            return mappedObject;
        });
        carousel.items = data;
        carousel.show_view_all = 1;

        carousel.deeplink = `doubtnutapp://downloadpdf_level_one?pdf_package=${carousel.type}`;
        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "MOCK TEST") {
        const sql = `SELECT cast(concat(package_id,level1) as char(50)) as id, package_id,package,level1,NULL as level2,NULL as location,class,NULL as status,package_order,'${carousel.type}' as type, level1 as title,img_url,'${description}' as description from pdf_download where package_id in (27) and class ='${studentClass}' and status=1 group by package_id, package,level1,class,package_order order by rand() limit ${limit}`;
        let data = await mysql.pool.query(sql).then((res) => res[0]);
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: false,
                show_video: false,
                image_url: elem.img_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: `doubtnutapp://downloadpdf_level_two?pdf_package=${elem.package}&level_one=${elem.level1}`,
                id: elem.id,
            };
            return mappedObject;
        });
        carousel.items = data;
        carousel.show_view_all = 1;
        carousel.deeplink = `doubtnutapp://downloadpdf_level_one?pdf_package=${carousel.type}`;
        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "JEE_MAINS_PY") {
        const sql = `SELECT cast(id as char(50)) as id,package_id,package,level1,level2,location,class,status,package_order,'${carousel.type}' as type, case when level1 is null then package else level1 end as title,img_url, '${actionActivity}' as actionActivity,'${description}' as description from pdf_download where package_id in (10) and class ='${studentClass}' and status=1 order by id desc limit ${limit}`;

        let data = await mysql.pool.query(sql).then((res) => res[0]);
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: false,
                show_video: false,
                image_url: elem.img_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: `doubtnutapp://pdf_viewer?pdf_url=https://d10lpgp6xz60nq.cloudfront.net/pdf_download/${elem.location}`,
                id: elem.id,
            };
            return mappedObject;
        });
        carousel.items = data;
        carousel.show_view_all = 0;
        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "JEE_ADV_PY") {
        const sql = `SELECT cast(id as char(50)) as id,package_id,package,level1,level2,location,class,status,package_order,'${carousel.type}' as type, case when level1 is null then package else level1 end as title,img_url,'${actionActivity}' as actionActivity,'${description}' as description from pdf_download where package_id in (9) and class ='${studentClass}' and status=1 order by id desc limit ${limit}`;
        let data = await mysql.pool.query(sql).then((res) => res[0]);
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: false,
                show_video: false,
                image_url: elem.img_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: `doubtnutapp://pdf_viewer?pdf_url=https://d10lpgp6xz60nq.cloudfront.net/pdf_download/${elem.location}`,
                id: elem.id,
            };
            return mappedObject;
        });
        carousel.items = data;
        carousel.show_view_all = 0;
        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "FORMULA_SHEET") {
        const sql = `SELECT cast(id as char(50)) as id,package_id,package,level1,level2,location,class,status,package_order,'${carousel.type}' as type,case when level2 is null then package else level2 end as title, img_url,'${actionActivity}' as actionActivity,'${description}' as description from pdf_download where package_id in (17) and class ='${studentClass}' and status=1 order by rand() limit ${limit}`;
        let data = await mysql.pool.query(sql).then((res) => res[0]);
        if (data.length) {
            carousel.deeplink = encodeURI(`doubtnutapp://downloadpdf_level_one?pdf_package=${data[0].package}`);
        }
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: false,
                show_video: false,
                image_url: elem.img_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: `doubtnutapp://pdf_viewer?pdf_url=https://d10lpgp6xz60nq.cloudfront.net/pdf_download/${elem.location}`,
                id: elem.id,
            };
            return mappedObject;
        });
        carousel.items = data;
        carousel.show_view_all = 1;

        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "CUTOFF_LIST") {
        const sql = `SELECT cast(id as char(50)) as id,package_id,package,level1,level2,location,class,status,package_order,'${carousel.type}' as type,case when level2 is null then package else level2 end as title,img_url,'${actionActivity}' as actionActivity,'${description}' as description from pdf_download where package_id in (18) and class ='${studentClass}' and status=1 and level1='2018' order by rand() limit ${limit}`;
        let data = await mysql.pool.query(sql).then((res) => res[0]);
        if (data.length) {
            carousel.deeplink = encodeURI(`doubtnutapp://downloadpdf_level_one?pdf_package=${data[0].package}`);
        }
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: false,
                show_video: false,
                image_url: elem.img_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: `doubtnutapp://pdf_viewer?pdf_url=https://d10lpgp6xz60nq.cloudfront.net/pdf_download/${elem.location}`,
                id: elem.id,
            };
            return mappedObject;
        });
        carousel.items = data;
        carousel.show_view_all = 1;

        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "12_BOARDS_PY") {
        const sql = `SELECT cast(id as char(50)) as id,package_id,package,level1,level2,location,class,status,package_order,'${carousel.type}' as type,case when level1 is null then package else level1 end as title, img_url,'${actionActivity}' as actionActivity,'${description}' as description from pdf_download where package_id in (11) and class ='${studentClass}' and status=1 order by id desc limit ${limit}`;
        let data = await mysql.pool.query(sql).then((res) => res[0]);
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: false,
                show_video: false,
                image_url: elem.img_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: `doubtnutapp://pdf_viewer?pdf_url=https://d10lpgp6xz60nq.cloudfront.net/pdf_download/${elem.location}`,
                id: elem.id,
            };
            return mappedObject;
        });
        carousel.items = data;
        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "SAMPLE_PAPERS") {
        let sql;
        if (studentClass === "12" || studentClass === "11") {
            sql = `SELECT cast(id as char(50)) as id,package_id,package,level1,level2,location,class,status,package_order,'${carousel.type}' as type,case when level1 is null then package else level1 end as title, img_url,'${actionActivity}' as actionActivity,'${description}' as description from pdf_download where package_id in (15) and class ='${studentClass}' and status=1 order by rand() limit ${limit}`;
        } else {
            sql = `SELECT cast(id as char(50)) as id,package_id,package,level1,level2,location,class,status,package_order,'${carousel.type}' as type,case when level1 is null then package else level1 end as title, img_url,'${actionActivity}' as actionActivity,'${description}' as description from pdf_download where package_id in (16) and class ='${studentClass}'and status=1 order by id desc limit ${limit}`;
        }
        let data = await mysql.pool.query(sql).then((res) => res[0]);
        if (data.length) {
            carousel.deeplink = encodeURI(`doubtnutapp://downloadpdf_level_one?pdf_package=${data[0].package}`);
        }
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: false,
                show_video: false,
                image_url: elem.img_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: `doubtnutapp://pdf_viewer?pdf_url=https://d10lpgp6xz60nq.cloudfront.net/pdf_download/${elem.location}`,
                id: elem.id,
            };
            return mappedObject;
        });
        carousel.items = data;

        carousel.show_view_all = 1;
        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "MOST_IMPORTANT_QUESTIONS") {
        let sql = "";
        if (studentClass == "11" || studentClass == "12") {
            sql = `SELECT cast(id as char(50)) as id,package_id,package,level1,level2,location,class,status,package_order,'${type}' as type,case when level1 is null then package else level1 end as title,img_url,'${actionActivity}' as actionActivity,'${description}' as description from pdf_download where package_id in (5) and class ='${studentClass}' and status=1 order by rand() limit ${limit}`;
        } else {
            sql = `SELECT cast(id as char(50)) as id,package_id,package,level1,level2,location,class,status,package_order,'${type}' as type,case when level1 is null then package else level1 end as title, img_url,'${actionActivity}' as actionActivity,'${description}' as description from pdf_download where package_id in (6) and class =${studentClass} and status=1 limit ${limit}`;
        }

        let data = await mysql.pool.query(sql).then((res) => res[0]);
        if (data.length) {
            carousel.deeplink = encodeURI(`doubtnutapp://downloadpdf_level_one?pdf_package=${data[0].package}`);
        }
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: false,
                show_video: false,
                image_url: elem.img_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: `doubtnutapp://pdf_viewer?pdf_url=https://d10lpgp6xz60nq.cloudfront.net/pdf_download/${elem.location}`,
                id: elem.id,
            };
            return mappedObject;
        });
        carousel.items = data;

        carousel.show_view_all = 1;
        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "IBPS_CLERK_SPECIAL") {
        const sql = `SELECT cast(id as char(50)) as id,package_id,package,level1,level2,location,class,status,package_order,'${carousel.type}' as type,case when level1 is null then package else level1 end as title, img_url,'${actionActivity}' as actionActivity,'${description}' as description from pdf_download where package_id in (3) and class ='${studentClass}'and status=1 order by rand() limit ${limit}`;
        let data = await mysql.pool.query(sql).then((res) => res[0]);
        if (data.length) {
            carousel.deeplink = encodeURI(`doubtnutapp://downloadpdf_level_one?pdf_package=${data[0].package}`);
        }
        // console.log(data);
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: false,
                show_video: false,
                image_url: elem.img_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: `doubtnutapp://pdf_viewer?pdf_url=https://d10lpgp6xz60nq.cloudfront.net/pdf_download/${elem.location}`,
                id: elem.id,
            };
            return mappedObject;
        });

        carousel.items = data;

        carousel.show_view_all = 1;
        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "SENT_UP_PAPER_2020") {
        const sql = `SELECT cast(id as char(50)) as id,package_id,package,level1,level2,location,class,status,package_order,'${carousel.type}' as type,case when level1 is null then package else level1 end as title, img_url,'${actionActivity}' as actionActivity,'${description}' as description from pdf_download where package_id in (30) and class ='${studentClass}'and status = 1`;
        let data = await mysql.pool.query(sql).then((res) => res[0]);
        if (data.length) {
            carousel.deeplink = null;
        }
        // console.log(data);
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: false,
                show_video: false,
                image_url: elem.img_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: `doubtnutapp://pdf_viewer?pdf_url=${elem.location}`,
                id: elem.id,
            };
            return mappedObject;
        });

        carousel.items = data;

        carousel.show_view_all = 1;
        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "NCERT_SOLUTIONS") {
        let sql = "";
        if (studentClass == "14") {
            sql = `SELECT cast(id as char(50)) as id,package_id,package,level1,level2,location,class,status,package_order,'${carousel.type}' as type,case when level1 is null then package else level1 end as title, img_url,'downloadpdf_level_two' as actionActivity,'${description}' as description from pdf_download where package_id in (13) and class ='${studentClass}' and status=1 group by level1 order by rand() limit ${limit}`;
        } else {
            sql = `SELECT cast(id as char(50)) as id,package_id,package,level1,level2,location,class,status,package_order,'${carousel.type}' as type,case when level2 is null then package else level2 end as title, img_url,'${actionActivity}' as actionActivity,'${description}' as description from pdf_download where package_id in (13) and class ='${studentClass}' and status=1 order by rand() limit ${limit}`;
        }
        let data = await mysql.pool.query(sql).then((res) => res[0]);
        if (data.length) {
            carousel.deeplink = encodeURI(`doubtnutapp://downloadpdf_level_one?pdf_package=${data[0].package}`);
        }
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: false,
                show_video: false,
                image_url: elem.img_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: `doubtnutapp://pdf_viewer?pdf_url=https://d10lpgp6xz60nq.cloudfront.net/pdf_download/${elem.location}`,
                id: elem.id,
            };
            return mappedObject;
        });
        carousel.items = data;
        carousel.show_all = 1;

        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "9_FOUNDATION_COURSE") {
        const sql = `SELECT cast(id as char(50)) as id,package_id,package,level1,level2,location,class,status,package_order,'${carousel.type}' as type,case when level1 is null then package else level1 end as title, img_url,'${actionActivity}' as actionActivity,'${description}' as description from pdf_download where package_id in (1) and class ='${studentClass}'and status=1 order by rand() limit ${limit}`;
        let data = await mysql.pool.query(sql).then((res) => res[0]);
        if (data.length) {
            carousel.deeplink = encodeURI(`doubtnutapp://downloadpdf_level_one?pdf_package=${data[0].package}`);
        }
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: false,
                show_video: false,
                image_url: elem.img_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: `doubtnutapp://pdf_viewer?pdf_url=https://d10lpgp6xz60nq.cloudfront.net/pdf_download/${elem.location}`,
                id: elem.id,
            };
            return mappedObject;
        });
        carousel.items = data;
        carousel.show_all = 1;

        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "10_BOARDS_PY") {
        const sql = `SELECT cast(id as char(50)) as id,package_id,package,level1,level2,location,class,status,package_order,'${carousel.type}' as type,case when level1 is null then package else level1 end as title, img_url,'${actionActivity}' as actionActivity,'${description}' as description from pdf_download where package_id in (14) and class ='${studentClass}' and status=1 order by id desc limit ${limit}`;
        let data = await mysql.pool.query(sql).then((res) => res[0]);
        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: elem.description,
                show_whatsapp: false,
                show_video: false,
                image_url: elem.img_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: `doubtnutapp://pdf_viewer?pdf_url=https://d10lpgp6xz60nq.cloudfront.net/pdf_download/${elem.location}`,
                id: elem.id,
            };
            return mappedObject;
        });
        carousel.items = data;
        carousel.show_all = 0;
        carousel.carousel_type = "carousel";
        return carousel;
    } if (carousel.type === "GAMES") {
        const sql = "select id, title, download_url, fallback_url, image_url from games where is_active=1 order by order_field asc";
        // console.log(sql);
        let data = await mysql.pool.query(sql).then((res) => res[0]);

        data = data.map((elem) => {
            const mappedObject = {
                title: elem.title,
                subtitle: "",
                show_whatsapp: true,
                show_video: false,
                image_url: elem.image_url,
                card_width: carousel.scroll_size, // percentage of screen width
                aspect_ratio: "",
                deeplink: encodeURI(`doubtnutapp://games?game_title=${elem.title}&game_url=${(_.isNull(elem.download_url) || elem.download_url.length === 0) ? elem.fallback_url : elem.download_url}`),
                id: elem.id,
            };
            return mappedObject;
        });
        carousel.items = data;
        carousel.show_all = 1;
        carousel.carousel_type = "carousel";
        carousel.deeplink = "doubtnutapp://dn_games";
        return carousel;
    } if (carousel.type === "AD_HOC_WIDGETS") {
        carousel.carousel_type = "real_time";
        carousel.android_widget = await getAdhocWidget(carousel);
        return carousel;
    } if (carousel.type === "AD_HOC_WIDGET_CHILD") {
        carousel.carousel_type = "real_time";
        carousel.android_widget = await getAdhocChildWidget(carousel);
        return carousel;
    } if (carousel.type === "REFERRAL_CEO_V2_TESTIMONIALS") {
        const testimonials = await getNameAndValueByBucket(carousel.data_type);
        const obj = JSON.parse(JSON.stringify(Data.referral_v2.default_json.referral_testimonial_widget[(carousel.locale == "hi") ? "hi" : "en"]));
        for (let i = 0; i < testimonials.length; i++) {
            const item = JSON.parse(JSON.stringify(Data.referral_v2.default_json.referral_testimonial_widget_item));
            item.data.id = testimonials[i].name;
            item.data.image_url = testimonials[i].value;
            item.data.deeplink = `doubtnutapp://video?qid=${testimonials[i].name}&page=REFERRAL_V2`;
            obj.widget_data.items.push(item);
        }
        carousel.carousel_type = "real_time";
        obj.order = carousel.caraousel_order;
        carousel.android_widget = obj;
        return carousel;
    } if (carousel.type === "AD_HOC_WIDGETS_AM" && carousel.data_type === "widget_classes_by_teacher") {
        carousel.carousel_type = "real_time";
        const itemsSql = carousel.secondary_data;
        const itemsData = await getSecondaryData(itemsSql);

        if (itemsData.length > 0) {
            carousel.facultyData = itemsData;
        }
        return carousel;
    } if (RealTimeTypes.includes(carousel.type)) {
        carousel.carousel_type = "real_time";
        return carousel;
    }
    carousel.items = [];
    carousel.carousel_type = "unknown";
    return carousel;
}

async function uploadToMongo(mongo, carouselData) {
    if (!carouselData.widget_type) {
        if (carouselData.scroll_type == "Horizontal") {
            carouselData.widget_type = "horizontal_list";
        } else if (carouselData.scroll_type == "Vertical") {
            carouselData.widget_type = "vertical_list_2";
        }
    }
    carouselData.carousel_id = carouselData.id;
    delete carouselData.id;
    console.log(carouselData);
    carouselData.class = parseInt(carouselData.class);
    const mongoUpsert = await mongo.collection("homepage_feed_test").replaceOne(
        { carousel_id: carouselData.carousel_id, class: carouselData.class, carousel_type: carouselData.carousel_type }, // query
        carouselData,
        {
            upsert: true,
        },
    );
    console.log(mongoUpsert);
}

async function start(job) {
    try {
        job.progress(0);
        const mongo = (await getMongoClient()).db("doubtnut");

        // Old Constants
        const language = "english";
        const weekNo = moment().format("ww");
        const baseUrl = `${config.staticCDN}q-thumbnail/`;
        const cdnUrl = `${config.staticCDN}images/`;
        const subjectUrl = [`${cdnUrl}physics_tricky.png`, `${cdnUrl}chemistry_tricky.png`, `${cdnUrl}maths_tricky.png`, `${cdnUrl}biology_tricky.png`, `${cdnUrl}default_tricky.png`];
        const actionActivity = "pdf_viewer";
        const buttonText = "";
        const buttonTextColor = "#000000";
        const buttonBgColor = "#f4e70c";
        const button = [buttonText, buttonTextColor, buttonBgColor];
        const durationTextColor = "#000000";
        const durationBgColor = "#ffffff";
        const duration = [durationTextColor, durationBgColor];
        const description = "";
        //

        // Get All ActiveCarousels
        const activeCarouselsSql = "select * from home_caraousels where is_active = 1 order by caraousel_order";
        const activeCarouselsData = await mysql.pool.query(activeCarouselsSql).then((res) => res[0]);

        // Start A synchronous  loop
        for (const activeCarousel of activeCarouselsData) {
            // Check If Carousel Belongs to a class or not
            console.log(activeCarousel);
            try {
                if (activeCarousel.class && activeCarousel.class !== "0") {
                    const carouselData = await carouselCreator(activeCarousel, activeCarousel.class, activeCarousel.data_limit, description, button, duration, actionActivity, subjectUrl, baseUrl, weekNo, language, activeCarousel.data_type);
                    if (carouselData.carousel_type == "real_time" || carouselData.type == "WHATSAPP_ASK" || carouselData.items.length > 0) {
                        const carouselDataClone = _.cloneDeep(carouselData);
                        await uploadToMongo(mongo, { ...carouselDataClone });
                    }
                } else {
                    // Loop For Each Class
                    for (const studentClass of classesArray) {
                        const carouselData = await carouselCreator(activeCarousel, studentClass, activeCarousel.data_limit, description, button, duration, actionActivity, subjectUrl, baseUrl, weekNo, language, activeCarousel.data_type);
                        carouselData.class = studentClass;
                        carouselData.classIteration = 1;
                        if (carouselData.carousel_type == "real_time" || carouselData.type == "WHATSAPP_ASK" || carouselData.items.length > 0) {
                            const carouselDataClone = _.cloneDeep(carouselData);
                            await uploadToMongo(mongo, { ...carouselDataClone });
                        }
                    }
                }
            } catch (err) {
                console.log(err);
            }
        }

        const verticalVideosSql = "select * from home_caraousels where is_active = 1 and type='AD_HOC_WIDGET_CHILD' and data_type in ('widget_parent#!#live_class_carousel_card_2','widget_parent_tab#!#live_class_carousel_card_2') and scroll_type = 'vertical' order by caraousel_order";
        const verticalVideosData = await mysql.pool.query(verticalVideosSql).then((res) => res[0]);

        for (const videoCarousel of verticalVideosData) {
            try {
                const videoData = await getVideoWidget(videoCarousel);
                if (videoData) {
                    const mongoUpsert = await mongo.collection("homepage_vertical_videos").replaceOne(
                        { carousel_id: videoData.carousel_id },
                        videoData,
                        {
                            upsert: true,
                        },
                    );
                    console.log(mongoUpsert);
                }
            } catch (err) {
                console.log(err);
            }
        }

        job.progress(100);
        return true;
    } catch (e) {
        console.log(e);
        return false;
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 * * * *", // * Every hour
};
