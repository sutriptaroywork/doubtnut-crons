/* eslint-disable import/no-dynamic-require */
/* eslint-disable global-require */
const Queue = require("bull");
const { setQueues, BullAdapter, router } = require("bull-board");
const fs = require("fs");
const express = require("express");
const parser = require("cron-parser");
const { config } = require("./modules");
const qList = require("./queues");

const supportedActions = ["now", "pause", "resume"];

const serviceToRun = process.argv[2];
console.log(serviceToRun);

async function start() {
    const dirs = fs.readdirSync("./src");
    for (let i = 0; i < dirs.length; i++) {
        const serviceName = dirs[i];
        if (serviceToRun && serviceName !== serviceToRun) {
            continue;
        }
        const service = require(`./src/${serviceName}/index`);

        const q = new Queue(serviceName, {
            redis: {
                host: config.queueRedis.host,
                password: config.queueRedis.password,
                db: 0,
            },
        });
        q.process(service.opts.concurrency || 1, async (job, done) => {
            if (job.opts.repeat && job.opts.repeat.cron && service.opts.skipDelayed) {
                const interval = parser.parseExpression(job.opts.repeat.cron, { tz: "Asia/Calcutta" });
                const next = new Date(interval.next().toISOString()).getTime();
                const prev = new Date(interval.prev().toISOString()).getTime();
                const now = new Date().getTime();
                const cronDiff = next - prev;
                const allowedDiff = Math.max(cronDiff * 0.01, 5000);
                const diff = Math.min(Math.abs(now - next), Math.abs(now - prev));
                if (diff > allowedDiff) {
                    console.log("Skipping delayed job", serviceName, diff, allowedDiff);
                    return done();
                }
            }
            try {
                console.log("Starting", serviceName, job.id);
                const output = await service.start(job);
                let err; let data;
                if (typeof output === "object") {
                    err = output.err;
                    data = output.data;
                }
                console.log("Done", serviceToRun, serviceName, job.id, err, data);
                if (!err) {
                    await job.progress(100);
                }
                done(err, data);
                if (serviceToRun) {
                    process.exit(0);
                }
            } catch (err) {
                console.log("error");
                console.log(err);
                // if (service.pagerdutyOpts) {
                //     createIncident(service.pagerdutyOpts.serviceId, service.pagerdutyOpts.escalationPolicyId, `${serviceName} failed`);
                // }
                done(err);
                if (serviceToRun) {
                    process.exit(1);
                }
            }
        });

        if (service.opts && service.opts.cron && typeof (service.start) === "function") {
            if (serviceName === serviceToRun) {
                console.log("Debug Starting", serviceName);
                q.add({ startTime: new Date() });
            }
        }
        qList.set(serviceName, new BullAdapter(q));
    }

    const queues = qList.get();
    if (!Object.keys(queues).length) {
        console.warn("No jobs");
        process.exit(2);
    }

    setQueues(Object.keys(queues).map((key) => queues[key]));

    if (serviceToRun) {
        return;
    }

    const app = express();

    const r = express.Router();
    r.get("/:qName/:action", (req, res) => {
        const { action, qName } = req.params;
        if (!supportedActions.includes(action)) {
            return res.json({ supportedActions });
        }
        const adapter = queues[qName];
        if (!adapter) {
            return res.send("Queue not found");
        }
        switch (action) {
            case "now":
                adapter.queue.add({ manualTrigger: true });
                res.send(`${qName} triggered now`);
                break;
            case "pause":
                adapter.queue.pause();
                res.send(`${qName} paused`);
                break;
            case "resume":
                adapter.queue.resume();
                res.send(`${qName} resumed`);
                break;
            default:
                res.send("OOPS!");
        }
    });

    app.use("/action", r);
    app.use("/dash", router);
    app.use("/", (_req, res) => res.redirect("/dash"));

    app.listen(5000);
}

start();
