const _ = require("lodash");
const moment = require("moment");
const { mysql } = require("../../modules/index");
const { config } = require("../../modules");
const RedisClient = require("../../modules/RedisGraph");

// TODO: remove cdn prefix  urls, cr, cd
// TODO: remove redisgraph.js
const RedisGraph = new RedisClient(config.redis.liveclass);

/*
  `assortment_id` int(11) NOT NULL AUTO_INCREMENT,
//   `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
//   `created_by` varchar(255) DEFAULT NULL,
  `class` int(11) NOT NULL,
  `ccm_id` tinyint(4) DEFAULT NULL,
  `display_name` varchar(255) DEFAULT NULL,
  `display_description` varchar(255) DEFAULT NULL,
  `category` varchar(255) DEFAULT NULL,
  `display_image_rectangle` varchar(255) DEFAULT NULL,
  `display_image_square` varchar(255) DEFAULT NULL,
  `deeplink` varchar(255) DEFAULT NULL,
  `max_retail_price` float NOT NULL,
  `final_price` float NOT NULL,
  `meta_info` varchar(255) DEFAULT NULL,
  `max_limit` float NOT NULL,
  `is_active` tinyint(4) DEFAULT NULL,
  `check_okay` tinyint(4) DEFAULT NULL,
  `start_date` timestamp NOT NULL,
  `end_date` timestamp NOT NULL,
  `expiry_date` timestamp NOT NULL,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
//   `updated_by` varchar(255) DEFAULT NULL,
  `priority` int(4) DEFAULT NULL,
  `dn_spotlight` tinyint(4) DEFAULT NULL,
  `promo_applicable` int(11) DEFAULT '0',
  `minimum_selling_price` float NOT NULL,
  `parent` int(11) DEFAULT NULL,
  `is_free` tinyint(4) DEFAULT '0',
  `assortment_type` varchar(50) DEFAULT NULL,
  `display_icon_image` varchar(200) DEFAULT NULL,
  `faculty_avatars` varchar(2000) DEFAULT NULL,
  `demo_video_thumbnail` varchar(500) DEFAULT NULL,
  `demo_video_qid` varchar(100) DEFAULT NULL,
  `rating` varchar(10) DEFAULT NULL,
  `subtitle` varchar(200) DEFAULT NULL,
  `sub_assortment_type` varchar(50) DEFAULT NULL,
  `year_exam` int(11) DEFAULT NULL,
  `category_type` varchar(50) DEFAULT NULL,
  `is_active_sales` int(11) DEFAULT '0',
  `is_show_web` tinyint(4) DEFAULT NULL,

*/
function getCourseDetails(limit, offset) {
    const sql = "select assortment_id,class,ccm_id,display_name,display_description,category,display_image_rectangle,display_image_square,deeplink,max_retail_price,final_price,meta_info,max_limit,is_active,check_okay,start_date,end_date,expiry_date,priority,dn_spotlight,promo_applicable,minimum_selling_price,parent,is_free,assortment_type,display_icon_image,faculty_avatars,demo_video_thumbnail,demo_video_qid,rating,subtitle,sub_assortment_type,year_exam,category_type,is_active_sales,is_show_web,updated_at from course_details group by assortment_id limit ? offset ?";
    return mysql.pool.query(sql, [limit, offset]).then((res) => res[0]);
}
/*
`id` int(11) NOT NULL AUTO_INCREMENT,
`resource_reference` varchar(255) DEFAULT NULL,
`resource_type` tinyint(4) NOT NULL,
`subject` varchar(200) DEFAULT NULL,
`topic` varchar(255) NOT NULL,
`expert_name` varchar(150) DEFAULT NULL,
`expert_image` varchar(255) DEFAULT NULL,
`q_order` int(11) DEFAULT NULL,
`class` int(11) DEFAULT NULL,
// `player_type` varchar(255) NOT NULL,
`meta_info` varchar(255) DEFAULT NULL,
`tags` varchar(1000) DEFAULT NULL,
`name` varchar(500) DEFAULT NULL,
`display` varchar(500) DEFAULT NULL,
`description` mediumtext,
`chapter` varchar(200) DEFAULT NULL,
`chapter_order` varchar(11) DEFAULT NULL,
`exam` varchar(100) DEFAULT NULL,
`board` varchar(100) DEFAULT NULL,
`ccm_id` int(11) DEFAULT NULL,
`book` varchar(100) DEFAULT NULL,
`faculty_id` int(11) DEFAULT NULL,
`stream_start_time` datetime DEFAULT NULL,
`image_url` varchar(255) DEFAULT NULL,
`locale` varchar(11) DEFAULT NULL,
`vendor_id` int(11) DEFAULT '1',
`duration` int(11) DEFAULT NULL,
// `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
// `created_by` varchar(50) DEFAULT NULL,
`rating` int(11) DEFAULT NULL,
`old_resource_id` int(11) DEFAULT NULL,
`stream_end_time` datetime DEFAULT NULL,
`stream_push_url` varchar(255) DEFAULT NULL,
`stream_vod_url` varchar(255) DEFAULT NULL,
`stream_status` enum('ACTIVE','INACTIVE') DEFAULT NULL,
`old_detail_id` int(11) DEFAULT NULL,
`lecture_type` varchar(50) DEFAULT NULL,
`is_active` tinyint(4) DEFAULT '1',
`updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
*/
function getCourseResources(limit, offset) {
    const sql = "select id,resource_reference,resource_type,subject,topic,expert_name,expert_image,q_order,class,meta_info,tags,name,display,description,chapter,chapter_order,exam,board,ccm_id,book,faculty_id,stream_start_time,image_url,locale,vendor_id,duration,rating,old_resource_id,stream_end_time,stream_push_url,stream_vod_url,stream_status,old_detail_id,lecture_type,is_active,updated_at from course_resources limit ? offset ?";
    return mysql.pool.query(sql, [limit, offset]).then((res) => res[0]);
}

/*
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `assortment_id` int(11) NOT NULL,
  `course_resource_id` int(11) NOT NULL,
  `resource_type` char(10) NOT NULL,//
//   `name` varchar(3000) DEFAULT NULL,
  `schedule_type` varchar(50) NOT NULL,//
  `live_at` timestamp NULL DEFAULT NULL,//
//   `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
//   `is_trial` int(11) DEFAULT NULL,
  `is_replay` int(11) DEFAULT NULL,//
//   `old_resource_id` int(11) NOT NULL,
//   `resource_name` varchar(1000) DEFAULT NULL,
  `batch_id` int(11) NOT NULL DEFAULT '1',//
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
*/

function getCourseResourcesMapping(limit, offset) {
    const sql = "select id,assortment_id,course_resource_id,resource_type,schedule_type,live_at,is_replay,batch_id,updated_at from course_resource_mapping limit ? offset ?";
    return mysql.pool.query(sql, [limit, offset]).then((res) => res[0]);
}
function createCourseDetailNode({ data }) {
    data.start_date = moment(data.start_date).unix();
    data.end_date = moment(data.end_date).unix();
    data.expiry_date = moment(data.expiry_date).unix();
    data.updated_at = moment(data.updated_at).unix();
    const query = "CREATE (:Assortment $data)";
    return RedisGraph.query(query, { data });
}

function createCourseResourceNode({ data }) {
    data.stream_start_time = moment(data.stream_start_time).unix();
    data.stream_end_time = moment(data.stream_end_time).unix();
    data.updated_at = moment(data.updated_at).unix();

    const query = "CREATE (:Resource $data)";
    return RedisGraph.query(query, { data });
}

function createAssortmentIndex() {
    const query = "CREATE INDEX ON :Assortment(assortment_id)";
    return RedisGraph.query(query);
}

function createResourceIndex() {
    const query = "CREATE INDEX ON :Resource(id)";
    return RedisGraph.query(query);
}

function createResourceReferenceIndex() {
    const query = "CREATE INDEX ON :Resource(resource_reference)";
    return RedisGraph.query(query);
}
function createCRMIndex() {
    const query = "CREATE INDEX FOR ()-[h:contains]->() ON (h.id)";
    return RedisGraph.query(query);
}
function createCourseResourceMappingRelation({ data }) {
    let query;
    if (data.resource_type === "assortment") {
        query = "Match (a:Assortment{assortment_id:$parentId}) Match (b:Assortment{assortment_id:$childId}) Create (a)-[:contains $relData]->(b)";
    } else {
        query = "Match (a:Assortment{assortment_id:$parentId}) Match (b:Resource{id:$childId}) Create (a)-[:contains $relData]->(b)";
    }
    const parentId = data.assortment_id;
    const childId = data.course_resource_id;
    const relData = _.omit(data, ["assortment_id", "course_resource_id"]);
    relData.live_at = moment(relData.live_at).unix();
    relData.updated_at = moment(relData.updated_at).unix();
    return RedisGraph.query(query, { relData, childId, parentId });
}

// get allthe videos for assorments
// GRAPH.QUERY liveclass "MATCH (r:Assortment{assortment_id:1036343}) -[*4]->(v:Resource) RETURN v"

// GRAPH.QUERY liveclass "MATCH (r:Assortment{assortment_id:1036343}) -[*3]->()-[:contains{resource_type:'resource',batch_id:3}]->(v:Resource) RETURN v limit 10"
// get all the subjects for assorments
// GRAPH.QUERY liveclass "MATCH (r:Assortment{assortment_id:1036343,batch_id:3}) -[*1]->(v:Assortment{assortment_type:'subject'}) RETURN v"

// get course assortment for a given resource id
// GRAPH.QUERY liveclass "MATCH (r:Resource{resource_reference:'643099981|643099982',batch_id:3}) <-[*]-(v:Assortment) RETURN v.assortment_id"

// subject
// GRAPH.QUERY liveclass "MATCH (r:Resource{cr_id:578423,batch_id:3}) <-[*]-(v:Assortment{assortment_type:'subject'}) RETURN v"

async function start() {
    try {
    // GRAPH.QUERY liveclass "CREATE INDEX ON :Assortment(assortment_id,batch_id)"
    // GRAPH.QUERY liveclass "CREATE INDEX ON :Resource(cr_id,batch_id)"
    // GRAPH.QUERY liveclass "CREATE INDEX FOR ()-[h:has]-() ON (h.crm_id)"
        const batchSize = 20000;
        const limit = batchSize;
        let offset = 0;
        let resultLength = -1;
        while (resultLength) {
            const cdData = await getCourseDetails(limit, offset);
            resultLength = cdData.length;
            offset += batchSize;
            const promises = [];
            cdData.forEach((cd) => {
                promises.push(createCourseDetailNode({ data: cd }));
            });
            await Promise.all(promises);
            console.log(offset);
        }
        console.log("course details done");
        offset = 0;
        resultLength = -1;
        while (resultLength) {
            const cdData = await getCourseResources(limit, offset);
            resultLength = cdData.length;
            offset += batchSize;
            const promises = [];
            cdData.forEach((cr) => {
                promises.push(createCourseResourceNode({ data: cr }));
            });
            await Promise.all(promises);
            console.log(offset);
        }
        console.log("course resources done");
        const indexPromises = [createResourceIndex(), createAssortmentIndex(), createResourceReferenceIndex()];
        await Promise.all(indexPromises);
        offset = 0;
        resultLength = -1;
        while (resultLength) {
            const cdData = await getCourseResourcesMapping(limit, offset);
            resultLength = cdData.length;
            offset += batchSize;
            const promises = [];
            cdData.forEach((crm) => {
                promises.push(createCourseResourceMappingRelation({ data: crm }));
            });
            await Promise.all(promises);
            console.log(offset);
        }
        const indexPromises1 = [createCRMIndex()];
        await Promise.all(indexPromises1);

        console.log("course resource mapping done");
        return true;
    } catch (err) {
        console.log(err);
        return false;
    }
}
// createCourseAssortmentTree(new Database(config.mysql_analytics),1036343, 3);

// createCourseAssortmentTree(1036343, 3);

module.exports.start = start;
module.exports.opts = {
    cron: "*/5 * * * *",
};
