/* eslint-disable max-classes-per-file */
/* eslint-disable valid-typeof */
/* eslint-disable class-methods-use-this */
const AbstractModel = require("./abstract_model");

class DeleteMediaRequest extends AbstractModel {
    constructor(params) {
        super();
        /**
         * Search text, which fuzzily matches the media file name or description. The more matching items and the higher the match rate, the higher-ranked the result. It can contain up to 64 characters.
         * @type {string || null}
         */
        this.FileId = (typeof params.FileId !== "undefined" && typeof params.FileId === "string") ? params.FileId : null;
    }

    checkArrayAndType(input, type, size) {
        try {
            if (Array.isArray(input) && input.length <= size) {
                return input.every((x) => typeof x === type);
            }
            return false;
        } catch (e) {
            console.log(e);
            return false;
        }
    }
}
class SearchMediaRequest extends AbstractModel {
    constructor(params) {
        super();
        /**
         * Search text, which fuzzily matches the media file name or description. The more matching items and the higher the match rate, the higher-ranked the result. It can contain up to 64 characters.
         * @type {string || null}
         */
        this.Text = (typeof params.Text !== "undefined" && typeof params.Text === "string") ? params.Text : null;

        /**
         * Tag set, which matches any element in the set.
<li>Tag length limit: 8 characters.</li>
<li>Array length limit: 10.</li>
         * @type {Array.<string> || null}
         */
        this.Tags = (typeof params.Tags !== "undefined" && this.checkArrayAndType(params.Tags, "string", 10)) ? params.Tags : null;

        /**
         * Category ID set, which matches the categories of the specified IDs and all subcategories. Array length limit: 10.
         * @type {Array.<number> || null}
         */
        this.ClassIds = (typeof params.ClassIds !== "undefined" && this.checkArrayAndType(params.ClassIds, "number", 10)) ? params.ClassIds : null;

        /**
         * Start time in the creation time range.
<li>After or at the start time.</li>
<li>In ISO 8601 format. For more information, please see [Notes on ISO Date Format](https://cloud.tencent.com/document/product/266/11732#I).</li>
         * @type {string || null}
         */
        this.StartTime = (typeof params.StartTime !== "undefined" && typeof params.StartTime === "string") ? params.StartTime : null;

        /**
         * End time in the creation time range.
<li>Before the end time.</li>
<li>In ISO 8601 format. For more information, please see [Notes on ISO Date Format](https://cloud.tencent.com/document/product/266/11732#I).</li>
         * @type {string || null}
         */
        this.EndTime = (typeof params.EndTime !== "undefined" && typeof params.EndTime === "string") ? params.EndTime : null;

        /**
         * Media file source. For valid values, please see [SourceType](https://cloud.tencent.com/document/product/266/31773#MediaSourceData).
         * @type {string || null}
         */
        this.SourceType = (typeof params.SourceType !== "undefined" && typeof params.SourceType === "string") ? params.SourceType : null;

        /**
         * [LVB code](https://cloud.tencent.com/document/product/267/5959) of a stream.
         * @type {string || null}
         */
        this.StreamId = (typeof params.StreamId !== "undefined" && typeof params.StreamId === "string") ? params.StreamId : null;

        /**
         * Unique ID of LVB recording file.
         * @type {string || null}
         */
        this.Vid = (typeof params.Vid !== "undefined" && typeof params.Vid === "string") ? params.Vid : null;

        /**
         * Sorting order.
<li>Valid value of `Sort.Field`: CreateTime</li>
<li>If `Text` is specified for the search, the results will be sorted by the match rate, and this field will not take effect</li>
         * @type {SortBy || null}
         */
        this.Sort = null;

        /**
         * <div id="p_offset">Start offset of a paged return. Default value: 0. Entries from No. "Offset" to No. "Offset + Limit - 1" will be returned.
<li>Value range: "Offset + Limit" cannot be more than 5,000. (For more information, please see <a href="#maxResultsDesc">Limit on the Number of Results Returned by API</a>)</li></div>
         * @type {number || null}
         */
        this.Offset = (typeof params.Offset !== "undefined" && typeof params.Offset === "number") ? params.Offset : null;

        /**
         * <div id="p_limit">Number of entries returned by a paged query. Default value: 10. Entries from No. "Offset" to No. "Offset + Limit - 1" will be returned.
<li>Value range: "Offset + Limit" cannot be more than 5,000. (For more information, please see <a href="#maxResultsDesc">Limit on the Number of Results Returned by API</a>)</li></div>
         * @type {number || null}
         */
        this.Limit = (typeof params.Limit !== "undefined" && typeof params.Limit === "number") ? params.Limit : null;

        /**
         * [Subapplication](/document/product/266/14574) ID in VOD. If you need to access a resource in a subapplication, enter the subapplication ID in this field; otherwise, leave it empty.
         * @type {number || null}
         */
        this.SubAppId = (typeof params.SubAppId !== "undefined" && typeof params.SubAppId === "number") ? params.SubAppId : null;
    }

    checkArrayAndType(input, type, size) {
        try {
            if (Array.isArray(input) && input.length <= size) {
                return input.every((x) => typeof x === type);
            }
            return false;
        } catch (e) {
            console.log(e);
            return false;
        }
    }
}

module.exports = {
    DeleteMediaRequest,
    SearchMediaRequest,
};
