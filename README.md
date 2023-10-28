# dn-crons

Repository to create/manage/monitor all the crons

### Steps to create a new cron
- Create a new directory in `src`. The directory name will be cron name.
- The cron directory should contain `index.js` which will server as starting point of the cron.
- `index.js` should contain a `start` function which needs to be async in nature. This function accepts a [`job`](https://github.com/OptimalBits/bull/blob/HEAD/REFERENCE.md#job) param. This `start` function should return `{ err, data }`
- This file should export 2 variables:
  - ```
    module.exports.start = start;
    ```
  - ```
    module.exports.opts = {
      cron: "<cron-time>",
      concurrency: 1, // optional
      removeOnComplete: 30, // optional
      removeOnFail: 30, // optional
    };
    ```
- Optional pagerduty variables:
  - ```
    module.exports.pagerdutyOpts = {
      serviceId: "<pagerduty-service-id>",
      escalationPolicyId: "<pagerduty-escalation-policy-id>"
    };
    ```
- The dashboard is available at [Bull Dashboard](https://bull.doubtnut.com/)
- The repository is containerized and deployed in K8S. New deployments can be done through Jenkins
- To test it locally, just run `npm start <service-name>`

### Documentation for creating sticky-notification table entries
- https://docs.google.com/document/d/1KUlLCZl-Hd0tFhhr9lbVWqNWDrfuLzRkqmJpOqBRWmI/edit?usp=sharing

### Active Crons
```
- studygroup-non-members - At 07:03 AM on every 3rd day
- studygroup_inactive_weekly - At 07:07 AM on every 3rd day
- studygroup-inactive-members - At 07:11 AM on every 3rd day
- khelo-jeeto-daily-leaderboard - At 12:08 AM everyday
- khelo-jeeto-weekly-leaderboard - At 12:12 AM on every Monday
- daily-goal-monthly-leaderboard - At 03:10 on day-of-month 1
- daily-goal-weekly-leaderboard - At 02:11 AM on every Monday
- p2p-v2-active-helpers - At 03:38 AM
- p2p-renotify - At every 15th minute from 5 through 59
- khelo-jeeto-active-yesterday - At 02:05 PM on every 2nd day
- khelo-jeeto-inactive-weekly- At 02:10 PM on every 2nd day
- khelo-jeeto-non-members - At 02:14 PM on every 2nd day
- trending-video-class - At 01:48 AM everyday
- Uninstall wa pitch d0 - At 07:03 PM everyday
- Uninstall wa pitch d3 - At 07:16 PM everyday
- inactive-no-login - Every Minute
- inactive-no-login-1-day - Every Minute
- inactive-no-login-1-hour - Every Minute
- inactive-no-login-3-day - Every Minute
- inactive-no-login-4-hour - Every Minute
- inactive-no-login-7-day - Every Minute
- inactive-no-login-10-day - Every Minute
- inactive-no-login-12-hour - Every Minute
- inactive-no-login-15-day - Every Minute
- adding-app-open-timestamp - At 02:05 AM everyday
-students-birthday-feed-auto-post At 12:01 AM Everyday
```


