const path = require("path");
const { URL } = require("url");
const crypto = require("crypto");
const _ = require("lodash");

const {
    config,
} = require("../../modules");

const { availableCDNs } = config;

const Data = {
    cdnHostLimelightStaticDONOTCHANGE: "https://doubtnut-static.s.llnwi.net",
    referral_v2: {
        video_widget_bg_color_array: ["#c19bff", "#f68b7d", "#f7e1ac", "#97d295", "#80bef7"],
        video_widget_image_url: `${config.staticCDN}referral_v2/e7d48e47-4fdb-4a8b-b1cc-33be628e03bd-3x.png`,
        goodie_widget_image_url: `${config.staticCDN}referral_v2/frame_188.png`,
        level_widget_phone_url: `${config.staticCDN}referral_v2/frame_189.png`,
        level_widget_lock_url: `${config.staticCDN}referral_v2/flat_color_icons_lock.png`,
        share_button_icon: `${config.staticCDN}engagement_framework/2977811D-9492-C710-0D13-AC564DDA0833.webp`,
        earn_more_widget_image_url: `${config.staticCDN}referral_v2/image_71.png`,
        level_widget_reward_image_url: `${config.staticCDN}referral_v2/reward_1.png`,
        course_calling_widget_image_url: `${config.staticCDN}referral_v2/call_center_agent_2.png`,
        referral_winner_congratulation_widget_bg_image_url: `${config.staticCDN}referral_v2/image_72.png`,
        course_calling_widget_bg_image_url: `${config.staticCDN}homepage-profile-icons/profile-section-bg.webp`,
        paytm: `${config.staticCDN}referral_v2/paytm.png`,
        goodie_form_animation_video: `${config.staticCDN}referral_v2/refer_video.zip`,
        share_contact: {
            keys: {
                key: "e#UVj$7cx-d@Y8m*v^qpxwdUr_T9TYYY",
                iv: "C#bzzMKrXbx2y#2v",
            },
            batch_size: 2000,
        },
        default_json: {
            referral_video_widget: {
                widget_data: {
                    full_width_cards: true,
                    items: [
                        {
                            type: "referral_video_widget",
                            data: {
                                title1: "",
                                title2: "",
                                title3: "",
                                bg_color: "",
                                image_url: "",
                                title4: {
                                    bg_color: "#000000",
                                    title: "",
                                    image_url: "",
                                },
                                bg_image_url: "", // for override
                                qid: "",
                                page: "REFERRAL_V2",
                                video_resource: {
                                    resource: "",
                                    video_resource: "",
                                    media_type: "BLOB",
                                },
                                default_mute: false,
                                auto_play: true,
                                auto_pause: false,
                            },
                            layout_config: {
                                margin_top: 0,
                                margin_bottom: 0,
                                margin_left: 0,
                                margin_right: 0,
                            },
                        },
                    ],
                    default_mute: false,
                    auto_play: true,
                    auto_pause: false,
                },
                widget_type: "widget_autoplay",
                layout_config: {
                    margin_top: 0,
                    margin_bottom: 0,
                    margin_left: 0,
                    margin_right: 0,
                },
            },
            referral_testimonial_widget_item: {
                type: "widget_library_card",
                data: {
                    id: 649241092,
                    page: "REFERRAL_V2",
                    image_url: "https://d10lpgp6xz60nq.cloudfront.net/q-thumbnail/649241092.png",
                    card_width: "1.2",
                    card_ratio: "16:9",
                    deeplink: "doubtnutapp://video?qid=649241092&page=REFERRAL_V2",
                    ocr_text: "",
                    background_color: "#FFEDD2",
                },
            },
            referral_steps_widget: {
                en: {
                    widget_type: "referral_steps",
                    data: {
                        heading: "<font color='#000000'>Win Phone</font> in just 3 simple steps!",
                        heading_color: "#504949",
                        steps: [
                            {
                                s_no: 1,
                                title: "Apne doston ko course khareedne ke liye <font color='#ee2db8'>referral code share</font>  share karo.",
                                highlight_color: "#ee2db8",
                            },
                            {
                                s_no: 2,
                                title: "Referral Code ko use karke <font color='#3941fc'>dost ko 30% discount</font> milega, aur aapko Rs. 1000 cashback!",
                                highlight_color: "#3941fc",
                            },
                            {
                                s_no: 3,
                                title: "2+ admission karane pe aap jeet sakte hai <font color='#0aac07'>Boat Airdopes, Bluetooth Speaker aur Redmi 9 Mobile Phone!</font>.",
                                highlight_color: "#0aac07",
                            },
                        ],
                    },
                    layout_config: {
                        margin_top: 16,
                        margin_bottom: 0,
                        margin_left: 16,
                        margin_right: 16,
                    },
                },
                hi: {
                    widget_type: "referral_steps",
                    data: {
                        heading: "3 आसान कदम और <font color='#000000'>आप जीतोगे फ़ोन !</font>",
                        heading_color: "#504949",
                        steps: [
                            {
                                s_no: 1,
                                title: "अपने दोस्तों को अपना <font color='#ee2db8'>रेफरल कूपन कोड भेजो </font> और कोर्स में उनका एडमिशन कराओ",
                                highlight_color: "#ee2db8",
                            },
                            {
                                s_no: 2,
                                title: "रेफरल कूपन कोड से आपके <font color='#3941fc'>दोस्त को मिलेगी 30% की बचत </font> और आपको मिलेगा Rs.1000 का Paytm इनाम",
                                highlight_color: "#3941fc",
                            },
                            {
                                s_no: 3,
                                title: "2+ एडमिशन करवाने पे आप जीत सकते है <font color='#0aac07'>Boat Airdopes, Bluetooth Speaker aur Redmi 9 Mobile Phone!</font>.",
                                highlight_color: "#0aac07",
                            },
                        ],
                    },
                    layout_config: {
                        margin_top: 16,
                        margin_bottom: 0,
                        margin_left: 16,
                        margin_right: 16,
                    },
                },
            },
            referral_calling_widget: {
                en: {
                    type: "course_calling_widget",
                    data: {
                        title: "<b>Hum jitayenge phone!</b><br>Aap bas humein call karo",
                        title_color: "#2f2f2f",
                        title_size: 14,
                        icon_url: `${config.staticCDN}referral_v2/call_center_agent_2.png`,
                        bg_image_url: `${config.staticCDN}homepage-profile-icons/profile-section-bg.webp`,
                        mobile: "01247158250",
                    },
                    layout_config: {
                        margin_top: 14,
                        margin_bottom: 0,
                        margin_left: 4,
                        margin_right: 4,
                    },
                },
                hi: {
                    type: "course_calling_widget",
                    data: {
                        title: "<b>हम जिताएंगे फ़ोन</b><br>आप बस हमे कॉल करो ",
                        title_color: "#2f2f2f",
                        title_size: 14,
                        icon_url: `${config.staticCDN}referral_v2/call_center_agent_2.png`,
                        bg_image_url: `${config.staticCDN}homepage-profile-icons/profile-section-bg.webp`,
                        mobile: "01247158250",
                    },
                    layout_config: {
                        margin_top: 14,
                        margin_bottom: 0,
                        margin_left: 4,
                        margin_right: 4,
                    },
                },
            },
            referral_goodie_widget: {
                en: {
                    type: "referral_goodie_widget",
                    data: {
                        title: `Pao <big><b>₹1000</b></big>  <img src='${config.staticCDN}referral_v2/paytm.png'> <big><b>cashback</b></big> every time your friend purchases a course`,
                        image_url: `${config.staticCDN}engagement_framework/61D890F0-34D4-60F0-A3A8-59FF9EED9C1E.webp`,
                        image_width: 128,
                    },
                    layout_config: {
                        margin_top: 14,
                        margin_bottom: 16,
                        margin_left: 16,
                        margin_right: 8,
                    },
                },
                hi: {
                    type: "referral_goodie_widget",
                    data: {
                        title: `हर दोस्त के एडमिशन पे पाओ <big><b>₹1000</b></big>  का <img src='${config.staticCDN}referral_v2/paytm.png'> <big><b>इनाम</b></big>`,
                        image_url: `${config.staticCDN}engagement_framework/61D890F0-34D4-60F0-A3A8-59FF9EED9C1E.webp`,
                        image_width: 128,
                    },
                    layout_config: {
                        margin_top: 14,
                        margin_bottom: 16,
                        margin_left: 16,
                        margin_right: 8,
                    },
                },
            },
            referral_level_widget: {
                type: "referral_level_widget",
                data: {
                    title: "",
                    levels: [],
                },
                layout_config: {
                    margin_top: 16,
                    margin_bottom: 0,
                    margin_left: 16,
                    margin_right: 16,
                },
            },
            referral_testimonial_widget: {
                en: {
                    type: "widget_parent",
                    widget_data: {
                        scroll_direction: "horizontal",
                        title: "Winner CEO ki Stories: 1 crore se zyada ka Paytm Cashback jeet chuke hai",
                        is_title_bold: true,
                        title_text_size: 16,
                        subtitle: "Roz dekho new winners ki stories.",
                        subtitle_text_size: 12,
                        subtitle_text_color: "#808080",
                        items: [],
                    },
                    layout_config: {
                        margin_top: 22,
                        margin_bottom: 0,
                        margin_left: 0,
                        margin_right: 0,
                    },
                    divider_config: {
                        background_color: "#e2e2e2",
                        height: 1,
                        margin_left: 16,
                        margin_right: 16,
                    },
                },
                hi: {
                    type: "widget_parent",
                    widget_data: {
                        scroll_direction: "horizontal",
                        title: "Winner CEO की कहानिया : 1 करोड़ से ज़्यादा का PayTm इनाम जीत चुके है !",
                        is_title_bold: true,
                        title_text_size: 16,
                        subtitle: "रोज़ देखो नए खिलाडी की कहानी ",
                        subtitle_text_size: 12,
                        subtitle_text_color: "#808080",
                        items: [],
                    },
                    layout_config: {
                        margin_top: 22,
                        margin_bottom: 0,
                        margin_left: 0,
                        margin_right: 0,
                    },
                    divider_config: {
                        background_color: "#e2e2e2",
                        height: 1,
                        margin_left: 16,
                        margin_right: 16,
                    },
                },
            },
            referral_faq_widget: {
                type: "course_faqs",
                data: {
                    title: "FAQ",
                    toggle: true,
                    items: [],
                },
                layout_config: {
                    margin_top: 0,
                    margin_bottom: 0,
                    margin_left: 0,
                    margin_right: 0,
                },
            },
            referral_earn_more_widget: {
                en: {
                    type: "referral_winner_earn_more_widget",
                    data: {
                        title1: "Want to earn more?",
                        title2: "Jeeto  <b>₹500</b> har baar jab bhi aapke referred dost kisi aur dost ko refer karte hai ",
                        image_url: `${config.staticCDN}referral_v2/image_71.png`,
                        deeplink: "",
                    },
                    layout_config: {
                        margin_top: 24,
                        margin_bottom: 0,
                        margin_left: 16,
                        margin_right: 16,
                    },
                },
                hi: {
                    type: "referral_winner_earn_more_widget",
                    data: {
                        title1: "और भी कमाना चाहते है? ",
                        title2: "जीतो  <b>₹500</b> हर बार जब भी आपके दोस्त किसी और दोस्त का एडमिशन करवाते है  ",
                        image_url: `${config.staticCDN}referral_v2/image_71.png`,
                        deeplink: "",
                    },
                    layout_config: {
                        margin_top: 24,
                        margin_bottom: 0,
                        margin_left: 16,
                        margin_right: 16,
                    },
                },
            },
            referral_winner_congratulation_widget: {
                en: {
                    type: "referral_winner_congratulation_widget",
                    data: {
                        start_color: "#f4ffdf",
                        end_color: "#90d178",
                        title1: "Congratulations!!",
                        title2: "Aapne Redmi 9 Phone jeeta hai",
                        image_url: `${config.staticCDN}referral_v2/frame_189.png`,
                        foreground_image_url: `${config.staticCDN}referral_v2/image_72.png`,
                    },
                    layout_config: {
                        margin_top: 0,
                        margin_bottom: 0,
                        margin_left: 0,
                        margin_right: 0,
                    },
                },
                hi: {
                    type: "referral_winner_congratulation_widget",
                    data: {
                        start_color: "#f4ffdf",
                        end_color: "#90d178",
                        title1: "बधाई हो !",
                        title2: "आपने Redmi 9 Phone जीता है !",
                        image_url: `${config.staticCDN}referral_v2/frame_189.png`,
                        foreground_image_url: `${config.staticCDN}referral_v2/image_72.png`,
                    },
                    layout_config: {
                        margin_top: 0,
                        margin_bottom: 0,
                        margin_left: 0,
                        margin_right: 0,
                    },
                },
            },
            referral_winner_earn_more_widget: {
                en: {
                    type: "referral_winner_earn_more_widget_v2",
                    data: {
                        title1: "Want to keep earning more?",
                        title2: "Aur bhi doston ka admission karao, aur aapko milte rahega cashback!",
                        title3: [
                            "<b>₹1000</b> milte rahega har referral pe.", "Pao <b>₹500</b> har baar jab bhi aapke referred dost kisi aur dost ko refer karte hai.(Keval 5 admission tak)",
                        ],
                        image_url: `${config.staticCDN}referral_v2/image_71.png`,
                    },
                    layout_config: {
                        margin_top: 16,
                        margin_bottom: 30,
                        margin_left: 16,
                        margin_right: 0,
                    },
                },
                hi: {
                    type: "referral_winner_earn_more_widget_v2",
                    data: {
                        title1: "और भी कमाना चाहते है? ",
                        title2: "और भी दोस्तों का एडमिशन कराओ और आपको मिलते रहेगा इनाम ",
                        title3: [
                            "<b>Rs. 1000</b> मिलते रहेगा हर एडमिशन करने पे", "जीतो <b>₹500</b> हर बार जब भी आपके दोस्त किसी और दोस्त का एडमिशन करवाते है(केवल 5 एडमिशन तक) ",
                        ],
                        image_url: `${config.staticCDN}referral_v2/image_71.png`,
                    },
                    layout_config: {
                        margin_top: 16,
                        margin_bottom: 30,
                        margin_left: 16,
                        margin_right: 0,
                    },
                },
            },
            referral_text_widget: {
                en: {
                    type: "text_widget",
                    data: {
                        html_title: "If you are facing any issues, please email us at ceosupport@doubtnut.com",
                        alignment: "center",
                        force_hide_right_icon: true,
                        deeplink: "doubtnutapp://email?email=ceosupport@doubtnut.com&subject=&message=",
                    },
                    layout_config: {
                        margin_top: 16,
                        margin_bottom: 0,
                        margin_left: 16,
                        margin_right: 16,
                    },
                },
                hi: {
                    type: "text_widget",
                    data: {
                        html_title: "अगर आपको कोई दिक्कत आ रही है, हमे ceosupport@doubtnut.com पे ईमेल करें",
                        alignment: "center",
                        force_hide_right_icon: true,
                        deeplink: "doubtnutapp://email?email=ceosupport@doubtnut.com&subject=&message=",
                    },
                    layout_config: {
                        margin_top: 16,
                        margin_bottom: 0,
                        margin_left: 16,
                        margin_right: 16,
                    },
                },
            },
            referral_claim_widget: {
                en: {
                    widget_data: {
                        title: "",
                        title_color: "#ff0000",
                        background_color: "#fff2f2",
                        border_color: "#ffffff",
                        subtitle: "",
                        subtitle_color: "#504949",
                        button_text: "Claim Your Goodies",
                        button_text_color: "#ffffff",
                        button_color: "#eb532c",
                        deeplink: "doubtnutapp://submit_address_dialog?type=referral_v2_goodie&id={id}",
                    },
                    widget_type: "validity_widget",
                    layout_config: {
                        margin_top: 0,
                        bg_color: "#ffffff",
                    },
                },
                hi: {
                    widget_data: {
                        title: "",
                        title_color: "#ff0000",
                        background_color: "#fff2f2",
                        border_color: "#ffffff",
                        subtitle: "",
                        subtitle_color: "#504949",
                        button_text: "अपना पता हमें दो",
                        button_text_color: "#ffffff",
                        button_color: "#eb532c",
                        deeplink: "doubtnutapp://submit_address_dialog?type=referral_v2_goodie&id={id}",
                    },
                    widget_type: "validity_widget",
                    layout_config: {
                        margin_top: 0,
                        bg_color: "#ffffff",
                    },
                },
            },
        },
    },
};

function getBarColorForRecentclassHomepage(subject) {
    const colorMap = {
        PHYSICS: "#6f0477",
        BIOLOGY: "#097704",
        CHEMISTRY: "#c85201",
        MATHS: "#047b79",
        // ENGLISH: '#1a99e9',
        // SCIENCE: '#0E2B6D',
        // GUIDANCE: '#0E2B6D',
        // ALL: '#0E2B6D',
        // TEST: '#54138a',
    };
    if (colorMap[subject]) {
        return colorMap[subject];
    }

    return "#750406";
}

const LiveclassData = {
    getBgImage(subject) {
        const obj = {
            physics: `${config.staticCDN}engagement_framework/65F1C900-A6C1-02EE-72D3-72BA36BE1F93.webp`,
            maths: `${config.staticCDN}engagement_framework/094FA1C2-0C6D-A8BE-F3DD-8E10D74EB0AE.webp`,
            biology: `${config.staticCDN}engagement_framework/9B437CC2-2FC2-67DC-33FA-C36484B24E5F.webp`,
            chemistry: `${config.staticCDN}engagement_framework/8A5867C6-E0C4-EABB-A0FB-A109429FE31C.webp`,
        };
        if (!obj[subject]) {
            return `${config.staticCDN}engagement_framework/097C25AB-880C-8552-0191-B31B77B6DA1A.webp`;
        }
        return obj[subject];
    },
    getcategoryIcons(category, versionCode) {
        const obj = {
            "State Boards": `${config.staticCDN}engagement_framework/7150360C-D0EC-9F34-7728-E9CBAC87F644.webp`,
            "UP Board": `${config.staticCDN}engagement_framework/1CE40272-2E13-75FC-9C65-7B5773B0AC88.webp`,
            "MP Board": `${config.staticCDN}engagement_framework/A86CF0BB-A876-35C5-C8BD-F0DC92FA31E8.webp`,
            "Rajasthan Board": `${config.staticCDN}engagement_framework/52049462-81BC-51D1-0970-A0857FEE761B.webp`,
            "Jharkhand Board": `${config.staticCDN}engagement_framework/634E0200-7BD5-3BD4-06BF-0545665DB165.webp`,
            "Chattisgarh Board": `${config.staticCDN}engagement_framework/E4E363FE-1967-2702-39DF-DA4445E76D93.webp`,
            "Uttarakhand Board": `${config.staticCDN}engagement_framework/D7E168D2-998B-D7BF-1F80-7B91B49B6E77.webp`,
            "Haryana Board": `${config.staticCDN}engagement_framework/36F38227-AA18-EDC0-6D64-E65942130378.webp`,
            "Himachal Board": `${config.staticCDN}engagement_framework/C404B51A-4A66-3DDA-71ED-7C68008FBAC5.webp`,
            "Maharashtra Board": `${config.staticCDN}engagement_framework/96FF6E5D-5CF2-82B2-0E0A-BC511ABECF91.webp`,
            "Gujarat Board": `${config.staticCDN}engagement_framework/06731EE4-044F-33D7-8DB3-FE6E13680372.webp`,
            "Bihar Board": `${config.staticCDN}engagement_framework/B46371FB-2B41-FD00-3AAE-2BEB5B5DDE11.webp`,
            IT: `${config.staticCDN}images/c14_category_it.webp`,
            Defence: `${config.staticCDN}images/c14_category_defence.webp`,
            SSC: `${config.staticCDN}images/c14_category_ssc.webp`,
            Teaching: `${config.staticCDN}images/c14_category_teaching.webp`,
            "Civil Services": `${config.staticCDN}images/c14_category_civilservices.webp`,
            "For All": `${config.staticCDN}images/c14_category_all.webp`,
            Railways: `${config.staticCDN}images/c14_category_railways.webp`,
            "State Police": `${config.staticCDN}images/c14_category_statepolice.webp`,
            "Other Boards": `${config.staticCDN}engagement_framework/7150360C-D0EC-9F34-7728-E9CBAC87F644.webp`,
        };
        const newIcons = {
            "State Boards": `${config.staticCDN}engagement_framework/7150360C-D0EC-9F34-7728-E9CBAC87F644.webp`,
            "UP Board": `${config.staticCDN}engagement_framework/DE6F3A9C-FDE8-6ECD-164B-D59F12B3DAA4.webp`,
            "MP Board": `${config.staticCDN}engagement_framework/DBB01166-ECA9-AADD-55EE-6B9D2A2201D3.webp`,
            "Rajasthan Board": `${config.staticCDN}engagement_framework/C6BFBDF6-CD68-8988-CF14-9006D2273F14.webp`,
            "Jharkhand Board": `${config.staticCDN}engagement_framework/79E85E37-46D2-5E2A-7F03-EC39D54A190D.webp`,
            "Chattisgarh Board": `${config.staticCDN}engagement_framework/5E3B2B7B-3AEF-7F37-0AD8-7428B9765C3B.webp`,
            "Uttarakhand Board": `${config.staticCDN}engagement_framework/96656B57-301E-2320-29B1-17A7D517BBAA.webp`,
            "Haryana Board": `${config.staticCDN}engagement_framework/ADF0DA4A-09C0-CE94-3BD1-197CE1F8B797.webp`,
            "Himachal Board": `${config.staticCDN}engagement_framework/0C2F7F9F-5E46-7654-A9A5-6903732E1C02.webp`,
            "Maharashtra Board": `${config.staticCDN}engagement_framework/83CE700A-F57E-5632-2604-B54B10E7B196.webp`,
            "Gujarat Board": `${config.staticCDN}engagement_framework/D0B97DBB-4D30-E97C-4FCE-A3F643A6C213.webp`,
            "Bihar Board": `${config.staticCDN}engagement_framework/48D6ACDA-C858-5B1D-3403-F2FA9F65FB8C.webp`,
            IT: `${config.staticCDN}engagement_framework/ED2B39A4-5E24-0828-2C97-1FCD7D02CC3B.webp`,
            Defence: `${config.staticCDN}engagement_framework/6D6F36FD-70E0-C06A-4099-E4157651EA62.webp`,
            SSC: `${config.staticCDN}engagement_framework/5D355396-A6EE-F66B-F8CC-7D5C629E00F1.webp`,
            Teaching: `${config.staticCDN}engagement_framework/A5658B88-1F03-146A-0607-5490F0560EB6.webp`,
            "Civil Services": `${config.staticCDN}engagement_framework/663DC12E-D401-21E1-367F-E7C51FEE2789.webp`,
            "For All": `${config.staticCDN}engagement_framework/072775C0-1287-AAEC-E0BE-BF00C7320C0E.webp`,
            Railways: `${config.staticCDN}engagement_framework/078B5398-6B1D-6753-7F9D-0AAD93D23ED7.webp`,
            "State Police": `${config.staticCDN}engagement_framework/123C76E7-ACDA-2A99-BF91-C0A7BA80507A.webp`,
            NDA: `${config.staticCDN}engagement_framework/343932A9-5E32-4123-742C-3F2CFB0BE3A3.webp`,
            "IIT JEE": `${config.staticCDN}engagement_framework/19A7B223-244D-A780-65DF-7ED6018C81BE.webp`,
            NEET: `${config.staticCDN}engagement_framework/7123A15D-A3F7-7718-0EBE-18A806944933.webp`,
            "CBSE Boards": `${config.staticCDN}engagement_framework/A92B998A-BE30-9EB0-4C67-5790509D23B5.webp`,
            "CBSE Board": `${config.staticCDN}engagement_framework/A92B998A-BE30-9EB0-4C67-5790509D23B5.webp`,
            "Kota Classes": `${config.staticCDN}engagement_framework/5869509F-22AA-AF7A-F579-9891E9AAE23F.webp`,
            "Other Boards": `${config.staticCDN}engagement_framework/0A587375-F3B1-FCBD-D195-DEC98811CC5F.webp`,
        };
        return versionCode > 934 ? newIcons[category] || `${config.staticCDN}engagement_framework/798806B1-0E7C-B5FC-D241-F6FBA613E406.webp` : obj[category] || `${config.staticCDN}engagement_framework/7150360C-D0EC-9F34-7728-E9CBAC87F644.webp`;
    },
    getBgImageForLiveCarousel(subject) {
        const obj = {
            physics: `${config.staticCDN}engagement_framework/5EF641EB-3545-EAD3-2F24-32AC07402186.webp`,
            maths: `${config.staticCDN}engagement_framework/152C1DFF-0DC6-52BD-3809-19C36C5925DD.webp`,
            biology: `${config.staticCDN}engagement_framework/B128A51A-6093-986A-B955-3A30640BB730.webp`,
            chemistry: `${config.staticCDN}engagement_framework/04299465-B3B8-4DCD-1D4C-5D984AD989CF.webp`,
            science: `${config.staticCDN}engagement_framework/2B3018C6-3713-D917-8D34-932C9BD243BD.webp`,
            "social science": `${config.staticCDN}engagement_framework/66DF8F45-FE69-71E1-0B79-B3D7A40ECF25.webp`,
            reasoning: `${config.staticCDN}engagement_framework/F0BEF98A-AB68-2F5A-EBBB-3AFD8779E22A.webp`,
            botany: `${config.staticCDN}engagement_framework/CC318456-A292-46AA-BE40-B7BC4069F9AD.webp`,
            english: `${config.staticCDN}engagement_framework/1372A484-3EF4-F831-BCCA-6AC81EB13D17.webp`,
            "english grammar": `${config.staticCDN}engagement_framework/1372A484-3EF4-F831-BCCA-6AC81EB13D17.webp`,
            "political science": `${config.staticCDN}engagement_framework/2B2ABD6C-E9B9-01D7-FA0F-DF3C56BFD91A.webp`,
            history: `${config.staticCDN}engagement_framework/1297ABB4-D163-A1D0-41B8-8E21376E38BD.webp`,
            geography: `${config.staticCDN}engagement_framework/FA091DD6-2DD2-1E5F-FADE-78D9DC343FE6.webp`,
            guidance: `${config.staticCDN}engagement_framework/5F264B82-DCCF-1EA0-F096-44DCF93D0EEE.webp`,
        };
        if (!obj[subject]) {
            return `${config.staticCDN}engagement_framework/DBA03800-0D76-0DA3-B032-721F6E3F6883.webp`;
        }
        return obj[subject];
    },
};

const getCDNUrlFromCDNOrigin = (origin, sendVideoCDNUrl) => {
    const originKey = sendVideoCDNUrl ? "cdn_video_origin" : "origin";
    const urlKey = sendVideoCDNUrl ? "cdn_video_url" : "url";
    for (let i = 0; i < availableCDNs.length; i++) {
        const obj = availableCDNs[i];
        if (obj[originKey] === origin) {
            return obj[urlKey];
        }
    }
    return null;
};

const getResourceFromCDNUrl = (origin, href, sendVideoCDNUrl, pathname) => {
    // * Get complete URL using Origin
    const cdnURL = getCDNUrlFromCDNOrigin(origin, sendVideoCDNUrl);
    if (!cdnURL) return null;
    // * Get only the resource path of the URL
    if (origin === Data.cdnHostLimelightStaticDONOTCHANGE) {
        return pathname.replace(/(\/static\/)|(\/static-imagekit\/)/, "/");
    }
    return pathname;
};

const getRandomCDNUrl = (sendVideoCDNUrl) => {
    // * Generate random number between 0 to 1
    if (!sendVideoCDNUrl) return config.staticCDN;
    const random = Math.random();
    for (let i = 0; i < availableCDNs.length; i++) {
        const obj = availableCDNs[i];
        // * Compare random with weights in data file and return cdn url accordingly
        if (random < obj.weight) {
            return sendVideoCDNUrl ? obj.cdn_video_url : obj.url;
        }
    }
    return sendVideoCDNUrl ? config.cdn_video_url : config.staticCDN;
};

function getTimeshiftUrl(domain, appName, streamName) {
    return `http://${domain}/timeshift/${appName}/${streamName}/timeshift.m3u8`;
}

function buildStaticCdnUrl(url, sendVideoCDNUrl = false) {
    // * Check for old static CDN, if it is old one, replace it with static CDN from env file
    if (!url) {
        return url;
    }
    const prefix = getRandomCDNUrl(sendVideoCDNUrl); // * Param whether to send video cdn url or not
    // const currentOrigin = (new URL(prefix)).origin;
    try {
        const { origin, href, pathname } = new URL(url);
        const resource = getResourceFromCDNUrl(origin, href, sendVideoCDNUrl, pathname);
        if (!resource) return url;
        url = new URL(path.join(prefix, resource)).href;
    } catch (e) {
        url = new URL(path.join(prefix, url)).href;
    }

    return url;
}

const hmac = (key, input) => {
    const hm = crypto.createHmac("sha256", key);
    hm.update(input);
    return hm.digest();
};

const urlSafeB64 = (input) => input.toString("base64").replace(/\//g, "_").replace(/\+/g, "-");

function getLicenseUrl(contentId, expiry = 300) {
    const timestamp = Math.floor(new Date().getTime() / 1000);
    const contentAuthObj = {
        contentId,
        expires: timestamp + expiry,
    };
    const signingDate = (new Date()).toISOString().replace(/[-.:]/g, "");
    const contentAuthStr = urlSafeB64(Buffer.from(JSON.stringify(contentAuthObj)));
    const signedDate = hmac(config.vdocipherApikey, signingDate);
    const hash = urlSafeB64(hmac(signedDate, contentAuthStr));
    const keyId = config.vdocipherApikey.substr(0, 16);
    const signature = `${keyId}:${signingDate}:${hash}`;
    const LICENSE_URL = `https://license.vdocipher.com/auth/wv/${urlSafeB64(Buffer.from(JSON.stringify({
        contentAuth: `${contentAuthStr}`,
        signature: `${signature}`,
    })))}`;
    return LICENSE_URL;
}

function generateVideoResourceObject(videoResource, timeout, questionID, versionCode, offsetEnabled = false) {
    const obj = {};
    let drmLicenseUrl = "";
    const drmScheme = "widevine";
    let offset = null;
    if (offsetEnabled && versionCode > 844) {
        offset = (typeof videoResource.video_offset !== "undefined") ? videoResource.video_offset : null;
    }
    obj.resource = videoResource.resource;
    obj.is_flv = false;
    obj.video_resource = videoResource.resource;
    obj.timeout = timeout;
    obj.drm_scheme = drmScheme;
    obj.media_type = videoResource.resource_type;
    let dropDownList = [];
    if (videoResource.resource_type === "DASH") {
        if (!_.isEmpty(videoResource.vdo_cipher_id) && !_.isNull(videoResource.vdo_cipher_id)) {
            drmLicenseUrl = getLicenseUrl(videoResource.vdo_cipher_id);
        }
        obj.resource = buildStaticCdnUrl(`${config.cdn_video_url}${videoResource.resource}`, true);
        if (videoResource.resource.includes("http")) {
            obj.resource = videoResource.resource;
        }
    } else if (videoResource.resource_type === "HLS") {
        if (!_.isEmpty(videoResource.vdo_cipher_id) && !_.isNull(videoResource.vdo_cipher_id)) {
            drmLicenseUrl = getLicenseUrl(videoResource.vdo_cipher_id);
        }
        obj.resource = buildStaticCdnUrl(`${config.cdn_video_url}${videoResource.resource}`, true);
        // check if tencent
        if (videoResource.resource.includes("vod2.myqcloud")) {
            obj.resource = videoResource.resource;
        }
        if (videoResource.resource.includes("http")) {
            obj.resource = videoResource.resource;
        }
    } else if (videoResource.resource_type === "RTMP") {
        const streamName = `${questionID}_H264xait`;
        const streamName2 = `${questionID}_480`;
        const streamName3 = `${questionID}_720`;
        dropDownList = [
            {
                display: "360",
                resource: `http://live.doubtnut.com/live/${streamName}.flv`,
                drm_scheme: "",
                drm_license_url: "",
                offset,
                media_type: "BLOB",
                timeout,
            },
            {
                display: "480",
                resource: `http://live.doubtnut.com/live/${streamName2}.flv`,
                drm_scheme: "",
                drm_license_url: "",
                offset,
                media_type: "BLOB",
                timeout,
            },
            {
                display: "720",
                resource: `http://live.doubtnut.com/live/${streamName3}.flv`,
                drm_scheme: "",
                drm_license_url: "",
                offset,
                media_type: "BLOB",
                timeout,
            },
        ];
        // add timeshift resource
        obj.time_shift_resource = {
            resource: getTimeshiftUrl(config.liveclass.vodDomain, config.liveclass.appName, questionID),
            drm_scheme: "",
            drm_license_url: "",
            media_type: "HLS",
            offset,
        };
    } else if (videoResource.resource_type === "BLOB") {
        obj.resource = buildStaticCdnUrl(`${config.cdn_video_url}${videoResource.resource}`, true);
        if (videoResource.resource.includes("http")) {
            obj.resource = videoResource.resource;
        }
    }

    obj.drop_down_list = dropDownList;
    obj.drm_license_url = drmLicenseUrl;
    if (videoResource.resource_type !== "RTMP") {
        obj.offset = offset;
    }
    if (videoResource.resource_type === "RTMP") {
        obj.resource = `http://live.doubtnut.com/live/${questionID}_H264xait.flv`;
        obj.video_resource = `http://live.doubtnut.com/live/${questionID}_H264xait.flv`;
        obj.timeout = timeout;
        obj.drm_scheme = drmScheme;
        obj.media_type = "BLOB";
        obj.is_flv = true;
    }
    return obj;
}

function showWhatsapp(carousel) {
    if (carousel.scroll_type == "Horizontal" && carousel.sharing_message) {
        return true;
    }
    return false;
}

function getTopperTestimonialWidget({
    carouselsData,
    result,
    locale,
}) {
    const items = [];
    for (let i = 0; i < result.length; i++) {
        items.push({
            deeplink: result[i].review_qid ? `doubtnutapp://video_dialog?question_id=${result[i].review_qid}&orientation=portrait&page=WHATS_NEW_HOME` : "",
            image_url: result[i].student_image,
        });
    }
    return {
        type: "testimonial_v2",
        data: {
            title: carouselsData.title,
            course_data: [{
                count_text: "200k+",
                benefit_text: locale === "hi" ? "छात्रों को पढ़ाया" : "Students taught",
            }, {
                count_text: "50k+",
                benefit_text: locale === "hi" ? "छात्रों ने 90%+ स्कोर किया" : "Students scored 90%+",
            }, {
                count_text: "200+",
                benefit_text: locale === "hi" ? "हर रोज लाइव क्लास" : "Live classes everyday",
            }],
            items,
        },
    };
}

function getTextColor(subject) {
    const obj = {
        physics: {
            text_color_primary: "#420146",
            text_color_secondary: "#ffffff",
            text_color_title: "#6f0477",
        },
        maths: {
            text_color_primary: "#004f4d",
            text_color_secondary: "#ffffff",
            text_color_title: "#047b79",
        },
        biology: {
            text_color_primary: "#034a01",
            text_color_secondary: "#ffffff",
            text_color_title: "#097704",
        },
        chemistry: {
            text_color_primary: "#a54503",
            text_color_secondary: "#ffffff",
            text_color_title: "#c85201",
        },
    };
    if (!obj[subject]) {
        return {
            text_color_primary: "#460600",
            text_color_secondary: "#ffffff",
            text_color_title: "#750406",
        };
    }
    return obj[subject];
}

module.exports = {
    Data,
    getBarColorForRecentclassHomepage,
    LiveclassData,
    availableCDNs,
    getCDNUrlFromCDNOrigin,
    getResourceFromCDNUrl,
    getRandomCDNUrl,
    generateVideoResourceObject,
    showWhatsapp,
    getTopperTestimonialWidget,
    getTextColor,
};
