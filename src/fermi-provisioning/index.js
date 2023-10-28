const rp = require("request-promise");
const { aws, mysql, config } = require("../../modules");

async function getUpcomingLCCount() {
    const sql = `SELECT count(*) as count from course_resources as b 
    left join liveclass_course_details as a on a.id=b.old_detail_id
    where b.resource_type = 4 and a.live_at between NOW() and NOW() + INTERVAL 15 MINUTE and a.is_replay = 0`;
    return mysql.pool.query(sql).then((res) => res[0][0].count);
}

// async function getInstanceCount() {
//     let count = 0;
//     const instances = await aws.ec2.describeInstances({
//         Filters: [{
//             Name: "tag:Name",
//             Values: [
//                 "fermi-rtmp",
//             ],
//         }],
//     }).promise();
//     for (let i = 0; i < instances.Reservations.length; i++) {
//         const reservations = instances.Reservations[i].Instances;
//         for (let j = 0; j < reservations.length; j++) {
//             const reservation = reservations[j];
//             if ([0, 16].indexOf(reservation.State.Code) > -1) {
//                 count++;
//             }
//         }
//     }
//     return count;
// }

async function launchInstances(amiId, instanceType, count) {
    if (count < 1) {
        return;
    }
    console.log(amiId, instanceType, count);
    const instanceParams = {
        ImageId: amiId,
        InstanceType: instanceType,
        KeyName: "fermi",
        MinCount: 1,
        MaxCount: count,
        SecurityGroupIds: ["sg-08889addffa3f56e8"],
        InstanceMarketOptions: {
            MarketType: "spot",
            SpotOptions: {
                InstanceInterruptionBehavior: "terminate",
            },
        },
        IamInstanceProfile: {
            Arn: "arn:aws:iam::942682721582:instance-profile/fermi-pipeline-role",
        },
        TagSpecifications: [
            {
                ResourceType: "instance",
                Tags: [
                    {
                        Key: "Name",
                        Value: "fermi-rtmp",
                    },
                ],
            },
        ],
    };

    try {
        await aws.ec2.runInstances(instanceParams).promise();
    } catch (e) {
        console.error(e);
    }
}

async function getStandbyNodesCount() {
    try {
        const nodes = await rp.get({
            url: "http://gateway.doubtnut.internal/api/fermi/rtmp/status?all=true&extendInactivity=true",
            json: true,
            timeout: 30000,
        });
        return nodes.filter((x) => !x.questionId).length;
    } catch (e) {
        console.error(e);
        return 0;
    }
}

async function start(job) {
    try {
        const lcCount = await getUpcomingLCCount();
        job.progress(33);
        const ec2Count = await getStandbyNodesCount();
        job.progress(66);
        const diff = lcCount - ec2Count;
        if (diff > 0) {
            await launchInstances(config.fermi.amiId, config.fermi.instanceType, Math.min(diff, config.fermi.maxInstanceCount));
        } else if (diff < 0) {
            // TODO terminate
            console.log((-1 * diff), "instances need termination");
        } else {
            console.log("Instance count at par");
        }
        await job.progress(100);
        return { data: "success" };
    } catch (err) {
        console.error(err);
        return { err };
    }
}

module.exports.start = start;
module.exports.opts = {
    cron: "10,25,40,55 6-22 * * *",
};
