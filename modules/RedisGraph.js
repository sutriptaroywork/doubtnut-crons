const Redis = require("ioredis");
const _ = require("lodash");

class RedisGraph {
    constructor({
        graphName, hosts, password, db,
    }) {
        this.client = hosts.length > 1
            ? new Redis.Cluster(hosts.map((host) => ({
                host,
                port: 6379,
            })), { redisOptions: { password, showFriendlyErrorStack: true } })
            : new Redis({
                host: hosts[0], port: 6379, password, showFriendlyErrorStack: true, db,
            });
        this.graphName = graphName;

        if (!this.graphName || this.graphName.length < 1) {
            throw new Error("Must specify a graph name in constructor");
        }

        // A single query returns an array with up to 3 elements:
        //  - the column names for each result
        //  - an array of result objects
        //  - some meta information about the query
        // A single result can be a node, relation, or scalar in the case of something like (id(node))
        Redis.Command.setReplyTransformer("GRAPH.QUERY", (result) => {
            const metaInformation = this.parseMetaInformation(result.pop());

            let parsedResults = [];
            parsedResults.meta = metaInformation;

            if (result.length > 1) { // if there are results to parse
                const columnHeaders = result[0];
                const resultSet = result[1];

                parsedResults = resultSet.map((item) => this.parseResult(columnHeaders, item));
            }

            return parsedResults;
        });
    }

    // eslint-disable-next-line class-methods-use-this
    parseMetaInformation(array) {
        const meta = {};
        for (const prop of array) {
            let [name, value] = prop.split(": ");
            if (value) {
                value = value.trim();
                meta[name] = value;
            }
        }
        return meta;
    }

    // a single result will consist of an array with one element for each returned object in the original QUERY
    // for example: "... RETURN n, l, p" <- will return multiple rows/records, each of which will have n, l, and p.
    // eslint-disable-next-line class-methods-use-this
    parseResult(columnHeaders, singleResult) {
        const columns = columnHeaders.map((columnHeader, index) => {
            const name = columnHeader;
            let value = singleResult[index];
            if (Array.isArray(value)) {
                value = _.fromPairs(value);
            }
            if (value.properties) {
                _.defaults(value, _.fromPairs(value.properties));
                delete value.properties;
            }
            try {
                return [name, JSON.parse(value)];
            } catch (error) {
                return [name, value];
            }
        });

        return _.fromPairs(columns);
    }

    query(theQuery, values) {
        let query;

        if (values) {
            for (const [key, value] of Object.entries(values)) {
                const theKey = `$${key}`;
                if (typeof value === "object") {
                    const stringifiedObject = JSON.stringify(value).replace(/"([^"]+)":/g, "$1:");
                    theQuery = theQuery.split(theKey).join(`${stringifiedObject}`);
                } else if (typeof value === "number") {
                    theQuery = theQuery.split(theKey).join(`${value}`);
                } else {
                    theQuery = theQuery.split(theKey).join(`"${value}"`);
                }
            }
        } else {
            query = theQuery;
        }
        query = theQuery;

        return this.client.call("GRAPH.QUERY", this.graphName, `${query}`);
    }

    delete() {
        return this.client.call("GRAPH.DELETE", this.graphName);
    }

    explain(command) {
        return this.client.call("GRAPH.EXPLAIN", this.graphName, `${command}`);
    }
}

module.exports = RedisGraph;
