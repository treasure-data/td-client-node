/**
 *  @author  jwyuan and lewuathe
 *  @license Apache-2.0
 *  @version 0.2.3
 *  @requires request
 *  @requires querystring
 *  @see http://docs.treasuredata.com/articles/rest-api-node-client
 *
 *  @example
 *  var TD = require('td');
 *  var client = new TD('TREASURE_DATA_API_KEY');
 *
 *  var fnPrint = function(err, results) {
 *    console.log(results);
 *  };
 *  client.listDatabase(function(err, results) {
 *    for (var i = 0; i < results.databases.length; i++) {
 *      client.listTables(results.databases[i].name, fnPrint);
 *    }
 *  });
 */

var request = require('request').defaults({encoding: 'utf8'});
var qs = require('querystring');
var merge = require('merge');

/**
 *  TreasureData REST API Client
 *  @constructor
 *  @param {string} apikey - The API key available from user account.
 *  @param {object} options - Specify the endpoint of TreasureData api.
 *                            options.host = 'api.treasuredata.com'
 *                            options.protocol = 'http'
 */
function TDClient(apikey, options) {
    this.options = options || {};
    this.options.apikey = apikey;
    this.options.host = this.options.host || 'api.treasuredata.com';
    this.options.protocol = this.options.protocol || 'http';
    this.options.headers = this.options.headers || {};
}

TDClient.prototype = {
    // PUBLIC METHOD
    get baseUrl() {
        return this.options.protocol + '://' + this.options.host;
    },


    /**
     *  Return the list of all databases belongs to given account
     *  @param {function} callback - Callback function which receives
     *                               error object and results object
     *  @example
     *  // Results object
     *  {name: 'db1', count: 1, created_at: 'XXX',
     *   updated_at: 'YYY', organization: null,
     *   permission: 'administrator'}
     */
    listDatabases: function(callback) {
        this._request("/v3/database/list", {
            method: 'GET',
            json: true
        }, callback);
    },

    /**
     *  Delete the given named database
     *  @param {string} db - The name of database
     *  @param {function} callback - Callback function which receives
     *                               error object and results object
     */
    deleteDatabase: function(db, callback) {
        this._request("/v3/database/delete/" + qs.escape(db), {
            method: 'POST',
            json: true
        }, callback);
    },

    /**
     *  Create the given named database
     *  @param {string} db - The name of database
     *  @param {function} callback - Callback function which receives
     *                               error object and results object
     */
    createDatabase: function(db, callback) {
        this._request("/v3/database/create/" + qs.escape(db), {
            method: 'POST',
            json: true
        }, callback);
    },

    /**
     *  Return the list of all tables belongs to given database
     *  @param {string} db - The name of database
     *  @param {function} callback - Callback function which receives
     *                               error object and results object
     */
    listTables: function(db, callback) {
        this._request("/v3/table/list/" + qs.escape(db), {
            json: true
        }, callback);
    },

    /**
     *  Create log type table in the given database
     *  @param {string} db - The name of database
     *  @param {string} table - The name of table
     *  @param {function} callback - Callback function which receives
     *                               error object and results object
     */
    createLogTable: function(db, table, callback) {
        this.createTable(db, table, 'log', callback);
    },

    /**
     *  Create item type table in the given database
     *  @param {string} db - The name of database
     *  @param {string} table - The name of table
     *  @param {function} callback - Callback function which receives
     *                               error object and results object
     */
    createItemTable: function(db, table, callback) {
        this.createTable(db, table, 'item', callback);
    },

    /**
     *  Create table in given database
     *  @param {string} db - The name of database
     *  @param {string} table - The name of table
     *  @param {string} type - The type of table ('log' or 'item')
     *  @param {function} callback - Callback function which receives
     *                               error object and results object
     */
    createTable: function(db, table, type, callback) {
        this._request("/v3/table/create/" + qs.escape(db) + "/" + qs.escape(table) + "/" + qs.escape(type), {
            method: 'POST',
            json: true
        }, callback);
    },

    /**
     *  Swap the content of two tables
     *  @param {string} db - The name of database
     *  @param {string} table1 - The first table
     *  @param {string} table2 - The second table
     *  @param {function} callback - Callback function which receives
     *                               error object and results object
     */
    swapTable: function(db, table1, table2, callback) {
        this._request("/v3/table/swap/" + qs.escape(db) + "/" + qs.escape(table1) + "/" + qs.escape(table2), {
            method: 'POST',
            json: true
        }, callback);
    },

    updateSchema: function(db, table, schema_json, callback) {
        this._request("/v3/table/update-schema/" + qs.escape(db) + "/" + qs.escape(table), {
            method: 'POST',
            body: schema_json,
            json: true
        }, callback);
    },

    deleteTable: function(db, table, callback) {
        this._request("/v3/table/update-schema/" + qs.escape(db) + "/" + qs.escape(table), {
            method: 'POST',
            json: true
        }, callback);
    },

    tail: function(db, table, count, to, from, callback) {
        if (typeof count === 'function') {
            callback = count;
            count = null;
        } else if (typeof to === 'function') {
            callback = to;
            to = null;
        } else if (typeof from === 'function') {
            callback = from;
            from = null;
        }

        var params = {
            // format: 'msgpack'
        };
        if (count) {
            params.count = count;
        }
        if (to) {
            params.to = to;
        }
        if (from) {
            params.from = from;
        }

        this._request("/v3/table/tail/" + qs.escape(db) + "/" + qs.escape(table), {
            method: 'GET',
            qs: params
        }, callback);
    },


    /**
     *  Return the list of all jobs run by your account
     *  @param {string} from - The start of the range of list
     *  @param {string} to - The end of the range of list
     *  @param {string} status - The status of returned jobs
     *  @param {function} callback - Callback function which receives
     *                               error object and results object
     *
     */
    listJobs: function(from, to, status, conditions, callback) {
        if (typeof from === 'function') {
            callback = from;
            from = 0;
        } else if (typeof to === 'function') {
            callback = to;
            to = null;
        } else if (typeof status === 'function') {
            callback = status;
            status = null;
        } else if (typeof conditions === 'function') {
            callback = conditions;
            conditions = null;
        }

        var params = {
            from: from
        };
        if (to) {
            params.to = to;
        }
        if (status) {
            params.status = status;
        }
        if (conditions) {
            params.conditions = conditions;
        }

        this._request("/v3/job/list/", {
            qs: params,
            json: true
        }, callback);
    },

    /**
     *  Returns the status and logs of a given job
     *  @param {string} job_id - The job id
     *  @param {function} callback - Callback function which receives
     *                               error object and results object
     *
     */
    showJob: function(job_id, callback) {
        this._request("/v3/job/show/" + qs.escape(job_id), {
            json: true
        }, callback);
    },

    /**
     *  Returns the result of a specific job.
     *  @param {string} job_id - The job id
     *  @param {function} callback - Callback function which receives
     *                               error object and results object
     */
    jobResult: function(job_id, callback) {
        this._request("/v3/job/result/" + qs.escape(job_id), {
            method: 'GET',
            qs: { format: 'tsv' }
        }, callback);
    },

    /**
     *  Kill the currently running job
     *  @param {string} job_id - the job id
     *  @param {function} callback - Callback function which receives
     *                               error object and results object
     */
    kill: function(job_id, callback) {
        this._request("/v3/job/kill/" + qs.escape(job_id), {
            method: 'POST',
            json: true
        }, callback);
    },

    /**
     *  Submit Hive type job
     *  @param {string} db - The name of database
     *  @param {string} query - The Hive query which run on given database
     *  @param {object} opts - Supported options are `result`,
     *                         `priority` and `retry_limit`
     *  @param {function} callback - Callback function which receives
     *                               error object and results object
     */
    hiveQuery: function(db, query, opts, callback) {
        if (typeof opts === 'function') {
            callback = opts;
            opts = {};
        }
        this._query(db, 'hive', query, opts, callback)
    },

    /**
     *  Submit Presto type job
     *  @param {string} db - The name of database
     *  @param {string} query - The Presto query which run on given database
     *  @param {object} opts - Supported options are `result`,
     *                         `priority` and `retry_limit`
     *  @param {function} callback - Callback function which receives
     *                               error object and results object
     */
    prestoQuery: function(db, query, opts, callback) {
        if (typeof opts === 'function') {
            callback = opts;
            opts = {};
        }
        this._query(db, 'presto', query, opts, callback)
    },

    // Export API
    export: function(db, table, storage_type, opts, callback) {
        if (typeof opts === 'function') {
            callback = opts;
            opts = {};
        }
        opts.storage_type = storage_type;

        this._request("/v3/export/run/" + qs.escape(db) + "/" + qs.escape(table), {
            method: 'POST',
            body: opts,
            json: true
        }, callback);
    },

    // Schedule API
    createSchedule: function(name, opts, callback) {
        if (!opts.cron || !opts.query) {
            return callback(new Error('opts.cron and opts.query is required!'), {});
        }
        opts.type = 'hive';
        this._request("/v3/schedule/create/" + qs.escape(name), {
            method: 'POST',
            body: opts,
            json: true
        }, callback);
    },

    deleteSchedule: function(name, callback) {
        this._request("/v3/schedule/delete/" + qs.escape(name), {
            method: 'POST',
            json: true
        }, callback);
    },

    listSchedules: function(callback) {
        this._request("/v3/schedule/list", {
            method: 'GET',
            json: true
        }, callback);
    },

    updateSchedule: function(name, params, callback) {
        this._request("/v3/schedule/update/" + qs.escape(name), {
            body: params,
            method: 'POST',
            json: true
        }, callback);
    },

    history: function(name, from, to, callback) {
        if (typeof from === 'function') {
            callback = from;
            from = 0;
        } else if (typeof from === 'function') {
            callback = to;
            to = null;
        }
        var params = {};
        if (from) {
            params.from = from;
        }
        if (to) {
            params.to = to;
        }

        this._request("/v3/schedule/history/" + qs.escape(name), {
            method: 'GET',
            qs: params,
            json: true
        }, callback);
    },

    runSchedule: function(name, time, num, callback) {
        if (typeof num === 'function') {
            callback = num;
            num = null;
        }

        var params = {};
        if (num) {
            params.num = num;
        }

        this._request("/v3/schedule/run/" + qs.escape(name) + "/" + qs.escape(time), {
            method: 'POST',
            qs: params,
            json: true
        }, callback);
    },

    // Import API
    import: function(db, table, format, stream, size, unique_id, callback) {
        if (typeof unique_id === 'function') {
            callback = unique_id;
            unique_id = null;
        }

        var path;
        if (unique_id) {
            path = "/v3/table/import_with_id/" + qs.escape(db) + "/" + qs.escape(table) + "/" + qs.escape(unique_id) + "/" + qs.escape(format);
        } else {
            path = "/v3/table/import/" + qs.escape(db) + "/" + qs.escape(table) + "/" + qs.escape(format);
        }

        this._put(path, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/octet-stream',
                'Content-Length': size
            }
        }, stream, callback);
    },

    // Result API
    listResult: function(callback) {
        this._request("/v3/result/list", {
            json: true
        }, callback);
    },

    createResult: function(name, url, callback) {
        this._request("/v3/result/create/" + qs.escape(name), {
            method: 'POST',
            body: { 'url': url },
            json: true
        }, callback);
    },

    deleteResult: function(name, url, callback) {
        this._request("/v3/result/delete/" + qs.escape(name), {
            method: 'POST',
            body: { 'url': url },
            json: true
        }, callback);
    },

    // Server Status API
    serverStatus: function(callback) {
        this._request("/v3/system/server_status", {
            json: true
        }, callback);
    },


    // Bulk import APIs
    createBulkImport: function(name, db, table, callback) {
        this._request('/v3/bulk_import/create/' + qs.escape(name) + '/' + qs.escape(db) + '/' + qs.escape(table), {
            method: 'POST',
            json: true
        }, callback);
    },

    deleteBulkImport: function(name, callback) {
        this._request('/v3/bulk_import/delete/' + qs.escape(name), {
            method: 'POST',
            json: true
        }, callback);
    },

    showBulkImport: function(name, callback) {
        this._request('/v3/bulk_import/show/' + qs.escape(name), {
            method: 'GET',
            json: true
        }, callback);
    },

    listBulkImports: function(callback) {
        this._request('/v3/bulk_import/list', {
            method: 'GET',
            json: true
        }, callback);
    },

    listBulkImportParts: function(name, callback) {
        this._request('/v3/bulk_import/list_parts/' + qs.escape(name), {
            method: 'GET',
            json: true
        }, callback);
    },

    bulkImportUploadPart: function(name, partName, stream, callback) {
        this._put('/v3/bulk_import/upload_part/' + qs.escape(name) + '/' + qs.escape(partName),
                  {json: true}, stream, callback);
    },

    bulkImportDeletePart: function(name, partName, callback) {
        this._request('/v3/bulk_import/delete_part/' + qs.escape(name) + '/' + qs.escape(partName), {
            method: 'POST',
            json: true
        }, callback);
    },

    performBulkImport: function(name, callback) {
        this._request('/v3/bulk_import/perform/' + qs.escape(name), {
            method: 'POST',
            json: true
        }, callback)
    },

    commitBulkImport: function(name, callback) {
        this._request('/v3/bulk_import/commit/' + qs.escape(name), {
            method: 'POST',
            json: true
        }, callback)
    },

    /**
     * _query: Protected method
     * @protected
     */
    _query: function(db, query_type, q, opts, callback) {
        opts.query = q;

        this._request("/v3/job/issue/" + query_type + "/" + qs.escape(db), {
            method: 'POST',
            body: opts,
            json: true
        }, callback);
    },

    /**
     * _request: Protected method
     * @protected
     */
    _request: function(path, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options  = {};
        }
        options.uri = this.baseUrl + path;
        options.headers = { 'Authorization': 'TD1 ' + this.options.apikey };
        // Merge custom headers
        options.headers = merge(options.headers, this.options.headers);

        callback = callback || noop;

        request(options, function (err, res, body) {
            if (err) { return callback(err, undefined); }
            if (res.statusCode < 200 || res.statusCode > 299) {
                return callback(new Error((body && body.error) || 'HTTP ' + res.statusCode),
                    body || {});
            }

            callback(null, body || {});
        });
    },

    /**
     *  _put: Protected method
     *  @protected
     */
    _put: function(path, options, stream, callback) {
        options.uri = this.baseUrl + path;
        options.headers = { 'Authorization': 'TD1 ' + this.options.apikey };

        stream.pipe(request.put(options, function (err, res, body) {
            if (err) { return callback(err, undefined); }
            if (res.statusCode < 200 || res.statusCode > 299) {
                return callback(new Error((body && body.error) || 'HTTP ' + res.statusCode),
                    body || {});
            }

            callback(null, body || {});
        }));
    }
};

module.exports = TDClient;

// Private and Utility Methods
function noop() {}
