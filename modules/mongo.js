const mongo = require("mongodb");
const config = require("./config");

let client;

async function connect() {
    client = await mongo.connect(config.mongo, {
        keepAlive: true,
        useNewUrlParser: true,
        useUnifiedTopology: true,
        connectTimeoutMS: 120000,
        maxIdleTimeMS: 12000,
    });
    return client;
}

/**
 * @returns {Promise<mongo.MongoClient>}
 */
function getClient() {
    if (client) {
        return client;
    }
    return connect();
}

module.exports = getClient;
