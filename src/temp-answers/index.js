/* eslint-disable no-await-in-loop */
const {
    mysql,
} = require("../../modules");

async function createTable() {
    const sql = `CREATE TABLE \`tmp_answers\` (
  \`question_id\` int(11) NOT NULL,
  \`answer_id\` int(11) NOT NULL,
  PRIMARY KEY (\`question_id\`),
  KEY \`question_id\` (\`question_id\`),
  KEY \`answer_id\` (\`answer_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8`;
    return mysql.writePool.query(sql).then(([res]) => res);
}

async function dropTable(tablename) {
    const sql = `DROP TABLE IF EXISTS ${tablename}`;
    return mysql.writePool.query(sql);
}

async function insertData() {
    const sql = "insert into tmp_answers select a.* from (Select question_id, max(answer_id) as answer_id from answers group by question_id) as a left join questions as b on a.question_id=b.question_id where is_answered=1 and matched_question is null";
    return mysql.writePool.query(sql);
}

async function start(job) {
    console.log('script started at: ', new Date());
    await dropTable("tmp_answers");
    console.log('tmp_answers deleted');
    await dropTable("tmp_answers_old");
    console.log('tmp_answers_old deleted');
    await createTable();
    console.log('tmp_answers created');
    await insertData();
    console.log('tmp_answers data inserted');
    const con = await mysql.writePool.getConnection();
    try {
        await con.query("START TRANSACTION");
        console.log('transaction started');
        await con.query("ALTER TABLE question_id_answer_mapping RENAME TO tmp_answers_old");
        console.log('question_id_answer_mapping renamed to tmp_answers_old');
        await con.query("ALTER TABLE tmp_answers RENAME TO question_id_answer_mapping");
        console.log('tmp_answers renamed to question_id_answer_mapping');
        await con.query("COMMIT");
        console.log('transaction committed');
        con.release();
        console.log('connection released');
    } catch (e) {
        console.error(e);
        await con.query("ROLLBACK");
        console.log('transaction rolledback ');
        con.release();
        throw e;
    }
    await dropTable("tmp_answers");
    console.log('tmp_answers dropped ');
    await dropTable("tmp_answers_old");
    console.log('tmp_answers_old dropped ');
    console.log('script ended at: ', new Date());
}

module.exports.start = start;
module.exports.opts = {
    cron: "0 3 * * *",
    removeOnComplete: 10,
    removeOnFail: 20,
};
