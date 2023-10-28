const Redis = require("ioredis");
const bluebird = require("bluebird");
const config = require("./config");

bluebird.promisifyAll(Redis);

const redisClient = config.redis.backend.hosts.length > 1
    ? new Redis.Cluster(config.redis.backend.hosts.map((host) => ({
        host,
        port: 6379,
    })), { redisOptions: { password: config.redis.backend.password, showFriendlyErrorStack: true } })
    : new Redis({
        host: config.redis.backend.hosts[0], port: 6379, password: config.redis.backend.password, showFriendlyErrorStack: true, db: config.redis.backend.db,
    });

module.exports = redisClient;
