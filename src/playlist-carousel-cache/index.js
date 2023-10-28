/* eslint-disable guard-for-in */
/* eslint-disable no-await-in-loop */
const _ = require("lodash");
const redisClient = require("../../modules/redis");
const { mysql } = require("../../modules");

async function getDistinctLibraryPlaylist() {
    const mysqlQ = "select * from library_playlists_ccm_mapping where is_active = 1 and flag_id is null";
    return mysql.pool.query(mysqlQ).then(([res]) => res);
}

async function getPlaylistQueries(libraryPlaylist) {
    const mysqlQ = "select * from new_library where id in (?)";
    return mysql.pool.query(mysqlQ, [libraryPlaylist]).then(([res]) => res);
}

async function getChildPlaylistQueries(libraryPlaylist) {
    const mysqlQ = "select nl1.id as parent_id, nl2.* from new_library nl1 inner join new_library nl2 on nl1.id = nl2.parent where nl2.is_last = 1 and nl1.id in (?)";
    return mysql.pool.query(mysqlQ, [libraryPlaylist]).then(([res]) => res);
}

async function runTgSql(mysqlQ) {
    const users = await mysql.pool.query(mysqlQ).then(([res]) => res);
    return users;
}

async function getTeacherImage(expertId) {
    const mysqlQ = "select image_url as expert_image from dashboard_users where id = ?";
    return mysql.pool.query(mysqlQ, [expertId]).then(([res]) => res);
}

async function start(job) {
    const result = await getDistinctLibraryPlaylist();
    let libraryPlaylist = _.map(result, (item) => item.playlist_id);
    libraryPlaylist = [...new Set(libraryPlaylist)];
    const mainPlaylist = await getPlaylistQueries(libraryPlaylist);
    if (mainPlaylist.length > 0) {
        const childPlaylist = [];
        const parentPlaylist = [];
        for (let i = 0; i < mainPlaylist.length; i++) {
            if (mainPlaylist[i].is_last == 1) {
                childPlaylist.push(mainPlaylist[i]);
            } else {
                parentPlaylist.push(mainPlaylist[i]);
            }
        }
        let parentPlaylistIds = _.map(parentPlaylist, (item) => item.id);
        parentPlaylistIds = [...new Set(parentPlaylistIds)];
        let playlistQueries = await getChildPlaylistQueries(parentPlaylistIds);
        playlistQueries = _.concat(playlistQueries, childPlaylist);
        playlistQueries = playlistQueries.filter((item, index, self) => self.findIndex((t) => t.id === item.id) === index);
        let allQuestionsFinal = [];
        if (playlistQueries.length > 0) {
            const workers = [];
            for (let i = 0; i < playlistQueries.length; i++) {
                const playlistQuery = playlistQueries[i].resource_path.replace(/\n/g, " ");
                workers.push(runTgSql(playlistQuery));
            }
            const allQuestions = await Promise.all(workers);
            for (let i = 0; i < allQuestions.length; i++) {
                const questions = allQuestions[i];
                if (questions.length > 0) {
                    const teacherImage = await getTeacherImage(questions[0].faculty_id);
                    questions.forEach((item) => {
                        item.expert_image = teacherImage[0].expert_image;
                        item.expert_name = playlistQueries[i].name;
                        item.child_playlist_id = playlistQueries[i].id;
                        item.parent_playlist_id = playlistQueries[i].parent_id;
                        item.playlist_order = playlistQueries[i].playlist_order;
                    });
                    allQuestionsFinal.push(...questions);
                }
            }
            allQuestionsFinal = _.sortBy(allQuestionsFinal, "playlist_order");
            console.log(allQuestionsFinal);
        }
        const parentGroup = _.groupBy(allQuestionsFinal, "parent_playlist_id");
        for (const key in parentGroup) {
            await redisClient.setAsync(`TOP_TEACHERS_PLAYLIST:${key}`, JSON.stringify(parentGroup[key]), "Ex", 60 * 60 * 7); // 7 hours
        }
    }
    job.progress(100);
    return {
        data: {
            done: true,
        },
    };
}

module.exports.start = start;
module.exports.opts = {
    cron: "30 */2 * * *", // * every 2 hours UTC
};
