const Redshift = require("node-redshift");
const config = require("./config");

const pool = new Redshift({
    user: config.redshift.user,
    database: config.redshift.database,
    password: config.redshift.password,
    port: config.redshift.port,
    host: config.redshift.host,
});

function query(sql) {
    return pool.query(sql, { raw: true });
}
module.exports = {
    query,
};
