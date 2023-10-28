const { kaleyra, mysql } = require("../../modules");

async function getlastFiveMinPurchases() {
    const sql = "select ps.id, ps.subscription_id, ps.student_id, s.mobile from (select id, student_id, subscription_id, new_package_id from payment_summary where created_at > date_sub(CURRENT_TIMESTAMP, INTERVAL 5 MINUTE)) as ps inner join (select * from student_package_subscription where is_active = 1) as sps on ps.subscription_id = sps.id inner join package as p on p.id = sps.new_package_id inner join (select assortment_id from course_details where assortment_type = 'course') as cd on cd.assortment_id= p.assortment_id left join students as s on s.student_id = ps.student_id group by ps.id";
    return mysql.pool.query(sql).then((res) => res[0]);
}

async function getlastFiveMinTrials() {
    const sql = "select distinct sps.student_id, s.mobile from student_package_subscription sps left join package p on sps.new_package_id=p.id left join students cs on cs.student_id=sps.student_id left join course_details cd on cd.assortment_id =p.assortment_id left join students as s on s.student_id = sps.student_id where sps.is_active=1 and cd.assortment_type like 'course' and sps.amount<0 and sps.created_at > date_sub(CURRENT_TIMESTAMP, INTERVAL 5 MINUTE)";
    return mysql.pool.query(sql).then((res) => res[0]);
}

function getNumbers(studentList) {
    const numArray = [];
    for (let i = 0; i < studentList.length; i++) {
        if (studentList[i].mobile !== null && studentList[i].mobile !== "") {
            let tempNumber = studentList[i].mobile.trim().replace(/" "/g, "").replace(/\n/g, "").replace(/\r/g, "");
            if (tempNumber.length > 10) {
                tempNumber = tempNumber.slice(-10);
            }
            const newNumber = `${tempNumber}`;
            numArray.push(newNumber);
        }
    }
    const mob = numArray.join();
    return mob;
}

function checkIfExists(studentId, purchaseDetails) {
    for (let i = 0; i < purchaseDetails.length; i++) {
        if (purchaseDetails[i].student_id == studentId) {
            return true;
        }
    }
    return false;
}

async function start(job) {
    try {
        let purchaseDetails = await getlastFiveMinPurchases();
        let trialDetails = await getlastFiveMinTrials();
        purchaseDetails = purchaseDetails.filter((item, index, self) => self.findIndex((t) => t.student_id === item.student_id) === index);
        trialDetails = trialDetails.filter((item, index, self) => self.findIndex((t) => t.student_id === item.student_id) === index);
        const tempTrial = [];
        for (let i = 0; i < trialDetails.length; i++) {
            const check = checkIfExists(trialDetails[i].student_id, purchaseDetails);
            if (!check) {
                tempTrial.push(trialDetails[i]);
            }
        }
        trialDetails = tempTrial;
        // for course purchase
        if (purchaseDetails && purchaseDetails.length) {
            const mob = getNumbers(purchaseDetails);
            const audio = "160440.ivr";
            await kaleyra.OBDcall(mob, audio);
        }
        // for trial activation
        if (trialDetails && trialDetails.length) {
            const mob = getNumbers(trialDetails);
            const audio = "160646.ivr";
            await kaleyra.OBDcall(mob, audio);
        }
        job.progress(100);
        return {
            data: {
                done: true,
            },
        };
    } catch (err) {
        console.log(err);
        return { err };
    }
}
module.exports.start = start;
module.exports.opts = {
    cron: "*/5 * * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
