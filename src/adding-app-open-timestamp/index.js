const _ = require("lodash");
const moment = require("moment");
const messages = require("../../modules/messages");
const {
    redshift,
    mysql,
} = require("../../modules");

async function getUsersWithNullReinstallTimestamp(limit, offset) {
    /** Getting Students Who Have Reinstall timestamp as null */
    const sql = "SELECT student_id,uninstall_timestamp,reinstall_timestamp,id FROM retarget_student_churn where reinstall_timestamp IS NULL and uninstall_timestamp <= CONCAT(DATE_SUB(CURDATE(), INTERVAL 1 DAY), ' 23:59:59') and uninstall_timestamp>= CONCAT(DATE_SUB(CURDATE(), INTERVAL 1 DAY), ' 00:00:00') order by id desc limit ? offset ?";
    return mysql.pool.query(sql, [limit, offset]).then((res) => res[0]);
}

async function getUserLatestAppOpenData(studentId, uninstallTimestamp) {
    const uninTimeInTimestampFormat = moment(uninstallTimestamp).format("YYYY-MM-DD HH:mm:ss");
    const sql = `select student_id,eventdate from analytics.app_opendn where student_id =${studentId} and eventdate > '${uninTimeInTimestampFormat}' order by eventdate asc limit 1`;
    return redshift.query(sql).then((res) => res);
}

async function addingAppOpenTimestamp(timestamp, studentId, uninstallTimestamp) {
    const appOpenTimestampInTimestampFormat = moment(timestamp).format("YYYY-MM-DD HH:mm:ss");
    const uninTimeInTimestampFormat = moment(uninstallTimestamp).format("YYYY-MM-DD HH:mm:ss");
    // updating app open timestamp column
    const sql = "Update retarget_student_churn set app_open_timestamp = ? where student_id= ? and uninstall_timestamp=?";
    return mysql.writePool.query(sql, [appOpenTimestampInTimestampFormat, studentId, uninTimeInTimestampFormat]).then((res) => res[0]);
}
async function checkingAppOpenTimestamp(students, counter) {
    for (const user of students) {
        try {
            console.log("counter>>", counter);
            const latestAppOpenDataOfUser = await getUserLatestAppOpenData(user.student_id, user.uninstall_timestamp);
            console.log("latestAppOpenDataOfUser>>", latestAppOpenDataOfUser);
            if (!_.isEmpty(latestAppOpenDataOfUser)) {
                await addingAppOpenTimestamp(latestAppOpenDataOfUser[0].eventdate, user.student_id, user.uninstall_timestamp);
            }
            counter++;
        } catch (e) {
            console.log("error", e);
        }
    }
    return true;
}

async function start(job) {
    try {
        job.progress(10);
        const chunk = 50000;
        job.progress(30);
        for (let i = 0, j = 1000000; i <= j; i += chunk) {
            const students = await getUsersWithNullReinstallTimestamp(chunk, i);
            console.log("students data>>>", students);
            if (_.isEmpty(students)) {
                console.log("No more data left");
                break;
            }
            await checkingAppOpenTimestamp(students, i);
        }

        job.progress(100);
        return true;
    } catch (e) {
        await messages.sendSms({
            phone: 9682632079,
            msg: `Dear user, Error occurred in doubtnut studygroup - adding-app-open-timestamp, Exception: ${e}`,
        });
        await messages.sendSms({
            phone: 8961383288,
            msg: `Dear user, Error occurred in doubtnut studygroup - adding-app-open-timestamp, Exception: ${e}`,
        });
        console.log("error", e);
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "30 19 * * *",
};
