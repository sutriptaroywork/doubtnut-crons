const { mysql} = require("../../modules/index");
const axios = require('axios');
const moment = require("moment");

async function getAmountToExpireInfoByDate(date) {
    const sql = `select wt.student_id as student_id, sum(wte.amount_left) as amount_to_expire from wallet_transaction wt join wallet_transaction_expiry wte on wte.wallet_transaction_id = wt.id where date(wt.expiry) = ?  and wte.status ="ACTIVE" and wt.expiry is not null and wte.wallet_transaction_ref_id is null group by 1 order by 2 desc;`;
    console.log(mysql.pool.format(sql,[date]));
    return mysql.pool.query(sql,[date]).then((res) => res[0]);
}

async function updateWalletExpiry(wt_id) {
    const sql = `update wallet_transaction_expiry set status = "EXPIRED" where wallet_transaction_ref_id = ?`;
    console.log(mysql.writePool.format(sql,[wt_id]));
    return mysql.writePool.query(sql,[wt_id]).then((res) => res[0]);
}

async function getWalletSummary(student_id) {
    const sql = `select * from wallet_summary where student_id = ?`;
    return mysql.pool.query(sql,[student_id]).then((res) => res[0]);
}

async function start(job) {

    let expireDate = moment().subtract(6,"hours").endOf('day').format("YYYY-MM-DD");
    console.log(expireDate);
    let studentInfo = await getAmountToExpireInfoByDate(expireDate)

    console.log("studentInfo", studentInfo);
    let unableToUpdateList = [];

    for(let i =0; i< studentInfo.length; i++)
    {
        if(studentInfo[i].amount_to_expire==0)
            continue;

        let walletSummary = await getWalletSummary(studentInfo[i].student_id);
        let reward_amount = walletSummary[0].reward_amount;

        if(parseInt(reward_amount) < parseInt(studentInfo[i].amount_to_expire)) {
            continue;
        }
          //debit this amount and update wallet_transaction_ref_id in wallet_expiry and update status as expired and debit with reason reward_expired
          let data = JSON.stringify({
              "student_id": studentInfo[i].student_id,
              "reward_amount": studentInfo[i].amount_to_expire,
              "type": "DEBIT",
              "payment_info_id": "dedsorupiyadega",
              "reason": "reward_expired"
          });

          let config = {
              method: 'post',
              url: 'https://micro.doubtnut.com/wallet/transaction/create',
              headers: {
                  'Content-Type': 'application/json',
              },
              data : data
          };

          try {
              const {data} = await axios(config);
              console.log(data);
              await updateWalletExpiry(data.data.id);
          }
        catch(e)
        {
            console.log(e);
            unableToUpdateList.push(studentInfo[i]);
        }

    }

    console.log(unableToUpdateList);

}

module.exports.start = start;
module.exports.opts = {
    cron: "10 0 * * *",
    removeOnComplete: 10,
    removeOnFail: 10,
};
