var nock = require('nock');

function TDMockApi(options) {
    this.options = options || {};
    this.baseUrl = this.options.baseUrl || 'http://mock.api.treasuredata.com';
    this.apiServer = nock(this.baseUrl);
}

TDMockApi.prototype = {
    _config: function() {
        this.apiServer.get('/v3/database/list').reply(200, {
            databases: [
                {name: 'db1', count: 1, created_at: '2011-07-23 16:32:52 UTC', updated_at: '2011-07-23 16:32:52 UTC', organization: null, permission: 'administrator'},
                {name: 'db2', count: 10, created_at: '2011-07-23 16:32:52 UTC', updated_at: '2011-07-23 16:32:52 UTC', organization: null, permission: 'administrator'},
                {name: 'db3', count: 20, created_at: '2011-07-23 16:32:52 UTC', updated_at: '2011-07-23 16:32:52 UTC', organization: null, permission: 'administrator'},
            ]
        });

        this.apiServer.post('/v3/database/delete/db_to_be_deleted').reply(200, {
           database: 'db_to_be_deleted'
        });

        this.apiServer.post('/v3/database/create/db_to_be_created').reply(200, {
           database: 'db_to_be_created'
        });

        this.apiServer.get('/v3/job/list/?from=0').reply(200, {
            count: 2,
            from: null,
            to: null,
            jobs: [
                {
                    "status": "success",
                    "job_id": "12345",
                    "created_at": "2013-11-13 19:39:19 UTC",
                    "updated_at": "2013-11-13 19:39:20 UTC",
                    "start_at": "2013-11-13 19:39:19 UTC",
                    "end_at": "2013-11-13 19:39:20 UTC",
                    "query": null,
                    "type": "hive",
                    "priority": 0,
                    "retry_limit": 0,
                    "hive_result_schema": null,
                    "result": "",
                    "url": "http://console.treasuredata.com/jobs/215782",
                    "user_name": "owner",
                    "organization": null,
                    "database": "database1"
                },
                {
                    "status": "success",
                    "job_id": "56789",
                    "created_at": "2013-11-13 19:32:45 UTC",
                    "updated_at": "2013-11-13 19:32:46 UTC",
                    "start_at": "2013-11-13 19:32:45 UTC",
                    "end_at": "2013-11-13 19:32:46 UTC",
                    "query": null,
                    "type": "bulk_import_perform",
                    "priority": 0,
                    "retry_limit": 0,
                    "hive_result_schema": null,
                    "result": "",
                    "url": "http://console.treasuredata.com/jobs/215781",
                    "user_name": "owner",
                    "organization": null,
                    "database": "database2"
                }
            ]
        });

        this.apiServer.get('/v3/job/show/12345').reply(200, {
            type: "hive",
            query: "SELECT * FROM www_access",
            job_id: "12345",
            status: "success",
            url: "http://console.treasuredata.com/job/12345",
            "created_at":"Sun Jun 26 17:39:18 -0400 2011",
            "updated_at":"Sun Jun 26 17:39:54 -0400 2011",
            "debug": {
                "stderr": "...",
                "cmdout": "..."
            }
        });

        this.apiServer.get('/v3/table/list/my_db').reply(200, {
            database: "my_db",
            tables: [
                {name: "access_log", count: 13123233},
                {name: "payment_log", count: 331232}
            ]
        });

        this.apiServer.post('/v3/table/swap/my_db/tbl1/tbl2').reply(200, {
            database: "my_db",
            table1: "tbl1",
            table2: "tbl2"
        });

        this.apiServer.post('/v3/job/issue/hive/my_db', {
            query: "SELECT COUNT(*) FROM www_access"
        }).reply(200, {
            job_id: "12345",
            type: "hive",
            database: "my_db",
            url: "http://console.treasure.com/will-be-ready"
        });

        this.apiServer.get('/v3/job/status/12345').reply(200, {
            job_id: "12345",
            status: "success",
            created_at: "2012-09-17 21:00:00 UTC",
            start_at: "2012-09-17 21:00:01 UTC",
            end_at: "2012-09-17 21:00:52 UTC"
        });

        this.apiServer.post('/v3/job/kill/12345').reply(200, {
            former_status: "running",
            job_id: "12345"
        });

        this.apiServer.post('/v3/schedule/run/12345/1448928000').reply(200, {
            jobs: [
                { job_id: "123456", type: 'hive', scheduled_at: '2015-12-01 00:00:00 UTC' }
            ]
        });
    },

    start: function() {
        this._config();
    }
};

module.exports = TDMockApi;
