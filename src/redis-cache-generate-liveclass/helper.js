const moment = require("moment");
const { mysql } = require("../../modules/index");

module.exports = class Course {
    static async getAllAssortmentsRecursively(assortmentList, totalAssortments = []) {
        try {
            if (assortmentList.length) {
                const results = await this.getChildAssortments(assortmentList);
                const now = moment();
                if (results.length) {
                    const assortmentListArr = results.filter((obj) => !obj.live_at || now.diff(obj.live_at, "days") <= 3).map((obj) => obj.course_resource_id);
                    totalAssortments = [...totalAssortments, ...assortmentListArr];
                    return this.getAllAssortmentsRecursively(assortmentListArr, totalAssortments);
                }
            }
            return { totalAssortments, assortmentList };
        } catch (e) {
            throw new Error(e);
        }
    }

    static async getChildAssortments(assortmentId) {
        const sql = "select x.*, y.live_at from (select course_resource_id, resource_type, live_at, class from course_resource_mapping as a inner join course_details as b on a.assortment_id=b.assortment_id where a.assortment_id in (?) and a.resource_type='assortment' group by course_resource_id) as x left join (select * from course_resource_mapping where resource_type='resource') as y on x.course_resource_id=y.course_resource_id";
        return mysql.pool.query(sql, [assortmentId]).then((res) => res[0]);
    }
};
