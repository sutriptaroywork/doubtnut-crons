const mysql = require("mysql2");
const config = require("./config");

const con = mysql.createPool({
    host: config.mysql.host.read,
    port: 3306,
    database: "classzoo1",
    user: config.mysql.user,
    password: config.mysql.password,
    connectionLimit: 200,
    enableKeepAlive: true,
    multipleStatements: true,
    queueLimit: 500,

});

const writeCon = mysql.createPool({
    host: config.mysql.host.write,
    port: 3306,
    database: "classzoo1",
    user: config.mysql.user,
    password: config.mysql.password,
    connectionLimit: 125,
    enableKeepAlive: true,
    multipleStatements: true,
    queueLimit: 500,
});

module.exports = {
    pool: con.promise(),
    writePool: writeCon.promise(),
};
