const rp = require("request-promise");
const config = require("./config");

function createIncident(serviceId, escalationPolicyId, title) {
    try {
        if (!config.prod) {
            return;
        }
        return rp.post("https://api.pagerduty.com/incidents", {
            headers: {
                accept: "application/vnd.pagerduty+json;version=2",
                from: "autobot@doubtnut.com",
                Authorization: `Token token=${config.pagerdutyKey}`,
            },
            body: {
                incident: {
                    title,
                    service: { id: serviceId, type: "service_reference" },
                    escalation_policy: { id: escalationPolicyId, type: "escalation_policy_reference" },
                },
            },
            json: true,
        });
    } catch (err) {
        console.error(err);
    }
}

module.exports = createIncident;
