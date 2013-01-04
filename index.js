var request = require('request').defaults({encoding: 'utf8'});
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
    list_databases: function(callback) {
        this._request("/v3/database/list", {
            method: 'GET',
            json: true
        }, callback);
    },

    delete_database: function(db, callback) {
        this._request("/v3/database/delete/" + qs.escape(db), {
            method: 'POST',
            json: true
        }, callback);
    },

    create_database: function(db, callback) {
        this._request("/v3/database/create/" + qs.escape(db), {
            method: 'POST',
            json: true
        }, callback);
    },

    // Tables API
    list_tables: function(db, callback) {
        this._request("/v3/table/list/" + db, {
            json: true
        }, callback);
    },

    // Jobs API
    show_job: function(job_id, callback) {
        this._request("/v3/job/show/" + job_id, {
            json: true
        }, callback);
    },

    hive_query: function(db, query, callback) {
        this._request("/v3/job/issue/hive/" + db, {
            method: 'POST',
            body: { query: query },
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
    }
};

module.exports = TDClient;

// Private and Utility Methods
function noop() {}

