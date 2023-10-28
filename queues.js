const queues = {};

function set(serviceName, adapter) {
    queues[serviceName] = adapter;
}

function get(serviceName) {
    if (!serviceName) {
        return queues;
    }
    return queues[serviceName];
}

module.exports = {
    set,
    get,
};
