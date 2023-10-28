const axios = require("axios");
const { config } = require("./index");

async function postMessage(message, room_list) {
    const headers = {
        // "x-auth-token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6NzI0NTE1LCJpYXQiOjE1OTY1MjIwNjgsImV4cCI6MTY1OTU5NDA2OH0.jCnoQt_VhGjC6EMq_ObPl9QpkBJNEAqQhPojLG_pz8c",
        "x-auth-token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MjM5MzU3OTE1LCJpYXQiOjE2NTg5NDUxNjAsImV4cCI6MTY1OTU0OTk2MH0.syOyTrf6ZmLMd201KsbmG4VVpWcCN6ojzc7fkP4_FHk",
        "Content-Type": "application/json",
        Cookie: "__cfduid=d117dc0091ddb32cee1131365a76a7c931617628174",
        version_code: 1026,
    };

    /*const headers = {
        "x-auth-token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MjEzMzE0Njk5LCJpYXQiOjE2NTg4MzQyNDEsImV4cCI6MTY1OTQzOTA0MX0.X_F1Dv_rqutN-LKM0XKz4-n4_SONzitlcoYXL8MjGkw",
        "Content-Type": "application/json",
        Cookie: "__cfduid=d117dc0091ddb32cee1131365a76a7c931617628174",
        version_code: 1026,
    };*/

    const data = { message: JSON.stringify(message), room_list };

    await axios.post(`${config.microUrl}api/study-group/post-multiple-groups`, data, { headers: headers }, { timeout: 5000 })
        .then((response) => {
            console.log(JSON.stringify(response.data));
        })
        .catch((error) => {
            console.log(error);
        });
}

function structureResponse(student, currentTime, type) {
    if (type === "image") {
        return {
            is_message: true,
            room_id: student.room_id,
            room_type: "study_group",
            message: {
                widget_data: {
                    child_widget: {
                        // widget_data: {
                        //     question_image: `${config.staticCloudfrontCDN}images/${student.question_image}`,
                        //     deeplink: `doubtnutapp://full_screen_image?ask_que_uri=${config.staticCloudfrontCDN}images/${student.question_image}&title=study_group`,
                        //     id: "question",
                        //     card_ratio: "16:9"
                        // },
                        // widget_type: "widget_asked_question"
                        widget_data: {
                            image_url: `${config.staticCloudfrontCDN}images/${student.question_image}`,
                            deeplink: `doubtnutapp://full_screen_image?ask_que_uri=${config.staticCloudfrontCDN}images/${student.question_image}&title=study_group`,
                            id: "image_card",
                            is_circle: false,
                            max_image_height: 350,
                            name: ""
                        },
                        widget_type: "image_card"
                    },
                    created_at: currentTime.valueOf(),
                    student_id: parseInt(student.student_id),
                    student_img_url: `${config.staticCloudfrontCDN}images/upload_45917205_1619087619.png`,
                    title: student.name,
                    sender_detail: "",
                    widget_display_name: "text_widget",
                    // "cta_text": "Register Now !",
                    // "deeplink": "doubtnutapp://course_details?id=scholarship_test_DNST32"
                },
                widget_type: "widget_study_group_parent"
            },
            // student_id: 98,
            // student_displayname: student.name,
            is_active: true,
            is_deleted: false,
            cdn_url: "https://d10lpgp6xz60nq.cloudfront.net/images/",
            is_admin: false
        };
    }

    if (type === "text") {
        return {
            is_message: true,
            room_id: student.room_id,
            room_type: "study_group",
            message: {
                widget_data: {
                    child_widget: {
                        widget_data: {
                            linkify: true,
                            title: student.title,
                        },
                        widget_type: "text_widget"
                    },
                    created_at: currentTime.valueOf(),
                    student_id: parseInt(student.student_id),
                    student_img_url: `${config.staticCloudfrontCDN}images/upload_45917205_1619087619.png`,
                    title: student.name,
                    sender_detail: "",
                    widget_display_name: "text_widget",
                    // "cta_text": "Register Now !",
                    // "deeplink": "doubtnutapp://course_details?id=scholarship_test_DNST32"
                },
                widget_type: "widget_study_group_parent"
            },
            // student_id: 98,
            // student_displayname: "Sutripta Roy",
            is_active: true,
            is_deleted: false,
            cdn_url: "https://d10lpgp6xz60nq.cloudfront.net/images/",
            is_admin: false
        };
    }
    return false;
}

module.exports = {
    postMessage,
    structureResponse,
};
