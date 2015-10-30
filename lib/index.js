var request = require('request').defaults({encoding: 'utf8'});
// var msgpack = require('msgpack');
var qs = require('querystring');

function TDClient(apikey, options) {
    this.options = options || {};
    this.options.apikey = apikey;
    this.options.host = this.options.host || 'api.treasure-data.com';
    this.options.protocol = this.options.protocol || 'http';
}

TDClient.prototype = {
    // PUBLIC METHODS
    get baseUrl() {
        return this.options.protocol + '://' + this.options.host;
    },

    // Database API
    listDatabases: function(callback) {
        this._request("/v3/database/list", {
            method: 'GET',
            json: true
        }, callback);
    },

    deleteDatabase: function(db, callback) {
        this._request("/v3/database/delete/" + qs.escape(db), {
            method: 'POST',
            json: true
        }, callback);
    },

    createDatabase: function(db, callback) {
        this._request("/v3/database/create/" + qs.escape(db), {
            method: 'POST',
            json: true
        }, callback);
    },

    // Tables API
    listTables: function(db, callback) {
        this._request("/v3/table/list/" + qs.escape(db), {
            json: true
        }, callback);
    },

    createLogTable: function(db, table, callback) {
        this.createTable(db, table, 'log', callback);
    },

    createItemTable: function(db, table, callback) {
        this.createTable(db, table, 'item', callback);
    },

    createTable: function(db, table, type, callback) {
        this._request("/v3/table/create/" + qs.escape(db) + "/" + qs.escape(table) + "/" + qs.escape(type), {
            method: 'POST',
            json: true
        }, callback);
    },

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


    // Jobs API
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

    showJob: function(job_id, callback) {
        this._request("/v3/job/show/" + qs.escape(job_id), {
            json: true
        }, callback);
    },

    jobResult: function(job_id, callback) {
        this._request("/v3/job/result/" + qs.escape(job_id), {
            method: 'GET',
            qs: { format: 'tsv' }
        }, callback);
    },

    kill: function(job_id, callback) {
        this._request("/v3/job/kill/" + qs.escape(job_id), {
            method: 'POST',
            json: true
        }, callback);
    },

    hiveQuery: function(db, query, callback) {
        this._request("/v3/job/issue/hive/" + qs.escape(db), {
            method: 'POST',
            body: { query: query },
            json: true
        }, callback);
    },

    prestoQuery: function(db, query, callback) {
        this._request("/v3/job/issue/presto/" + qs.escape(db), {
            method: 'POST',
            body: { query: query },
            json: true
        }, callback);
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
            qs: params,
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
            method: 'GET',
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


    // PROTECTED METHODS
    _request: function(path, options, callback) {
        if (typeof options === 'function') {
            callback = options;
            options  = {};
        }
        options.uri = this.baseUrl + path;
        options.headers = { 'Authorization': 'TD1 ' + this.options.apikey };
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

    _put: function(path, options, stream, callback) {
        options.uri = this.baseUrl + path;
        options.headers = { 'Authorization': 'TD1 ' + this.options.apikey };

        stream.pipe(request(options, function (err, res, body) {
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
