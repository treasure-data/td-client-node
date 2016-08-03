var nock = require('nock');
var assert = require('assert');

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

        this.apiServer.post('/v3/table/create/db/test_log_table/log').reply(200, {
          table: 'test_log_table',
          type: 'log',
          database: 'kai_test_db'
        });

        this.apiServer.post('/v3/table/create/db/test_item_table/item').reply(422, {
          error: 'Table type must be \'log\'',
          text: 'Table type must be \'log\'',
          severity: 'error'
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

        this.apiServer.get('/v3/job/result/12345?format=csv').reply(200,
          "Nobita,14,Tokyo\nTakeshi,14,Tokyo\nSuneo,14,Shizuoka"
        );

        this.apiServer.get('/v3/job/result/12345?format=tsv').reply(200,
          "Nobita\t14\tTokyo\nTakeshi\t14\tTokyo\nSuneo\t14\tShizuoka"
        );

        this.apiServer.get('/v3/job/result/12345?format=json').reply(200,
          "[Nobita,14,Tokyo]\n[Takeshi,14,Tokyo]\n[Suneo,14,Shizuoka]"
        );

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

        this.apiServer.post('/v3/table/delete/my_db/tbl_to_be_deleted').reply(200, {
           database: 'my_db',
           table: 'tbl_to_be_deleted',
           type: 'log'
        });

        var hive_query_response = {
            job_id: "12345",
            type: "hive",
            database: "my_db",
            url: "http://console.treasure.com/will-be-ready"
        }

        this.apiServer.post('/v3/job/issue/hive/my_db', {
            query: "SELECT COUNT(*) FROM www_access"
        }).reply(200, hive_query_response);

        this.apiServer.post('/v3/job/issue/hive/my_db', {
            query: "SELECT COUNT(*) FROM www_access",
            result: 'web://result.example.com/callback'
        }).reply(200, hive_query_response);

        var presto_query_response = {
            job: '12345',
            database: 'my_db',
            job_id: '12345'
        }

        this.apiServer.post('/v3/job/issue/presto/my_db', {
            query: "SELECT COUNT(*) FROM www_access"
        }).reply(200, presto_query_response);

        this.apiServer.post('/v3/job/issue/presto/my_db', {
            query: "SELECT COUNT(*) FROM www_access",
            result: 'web://result.example.com/callback'
        }).reply(200, presto_query_response);

        this.apiServer.post('/v3/schedule/create/my_scheduled_query', {
            cron: '0 * * * *',
            query: 'SELECT COUNT(1) FROM www_access',
            type: 'hive',
            database: 'sample_datasets'
        }).reply(200, {
            name: 'my_scheduled_query',
            cron: '0 * * * *',
            timezone: 'UTC',
            delay: 0,
            created_at: '2015-12-08T01:22:27Z',
            type: 'hive',
            query: 'SELECT COUNT(1) FROM www_access',
            database: 'sample_datasets',
            user_name: 'test@example.com',
            priority: 0,
            retry_limit: 0,
            result: '',
            id: 12345,
            start: '2015-12-08T02:00:00Z'
        });

        this.apiServer.post('/v3/schedule/delete/my_scheduled_query').reply(200, {
            name: 'my_scheduled_query',
            timezone: 'Asia/Tokyo',
            delay: 0,
            created_at: '2015-12-06T05:43:42Z',
            type: 'hive',
            query: 'SELECT COUNT(1) FROM www_access',
            database: 'sample_datasets',
            user_name: 'test@example.com'
        });

        this.apiServer.get('/v3/schedule/list').reply(200, {
            schedules: [
                {
                    name: 'my_scheduled_query',
                    cron: '0 * * * *',
                    timezone: 'Asia/Tokyo',
                    delay: 0,
                    created_at: '2015-12-06T05:43:42Z',
                    type: 'hive',
                    query: 'SELECT COUNT(1) FROM www_access',
                    database: 'sample_datasets',
                    user_name: 'test@example.com',
                    priority: 0,
                    retry_limit: 0,
                    next_time: null
                }
            ]
        });

        this.apiServer.post('/v3/schedule/update/my_scheduled_query', {
            query: 'SELECT COUNT(1) FROM nasdaq'
        }).reply(200, {
            name: 'my_scheduled_query',
            cron: '0 * * * *',
            timezone: 'UTC',
            delay: 0,
            created_at: '2015-12-08T01:22:27Z',
            type: 'hive',
            query: 'SELECT COUNT(1) FROM nasdaq',
            database: 'sample_datasets',
            user_name: 'test@example.com',
            priority: 0,
            retry_limit: 0,
            result: '',
            id: 12345,
            start: '2015-12-08T02:00:00Z'
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

        this.apiServer.get('/v3/job/show/custom_header').reply(function(uri, request) {
            assert.equal(this.req.headers['custom header'], 'CUSTOM');
        });

        this.apiServer.post('/v3/bulk_import/create/test_import/test_db/test_table').reply(200, {
            name: 'test_import',
            bulk_import: 'test_import'
        });

        this.apiServer.post('/v3/bulk_import/delete/test_import').reply(200, {
            name: 'test_import',
            bulk_import: 'test_import'
        });

        var bulkImport = {
            name: 'test_import',
            status: 'uploading',
            job_id: null,
            valid_records: null,
            error_records: null,
            valid_parts: null,
            error_parts: null,
            upload_frozen: false,
            database: 'test_db',
            table: 'test_table'
        }

        this.apiServer.get('/v3/bulk_import/show/test_import').reply(200, bulkImport);

        this.apiServer.get('/v3/bulk_import/list').reply(200, {
            bulk_imports: [
                bulkImport,
                bulkImport
            ]
        });

        this.apiServer.get('/v3/bulk_import/list_parts/test_import').reply(200, {
            parts: [ 'part1', 'part2' ],
            name: 'test_import',
            bulk_import: 'test_import'
        });

        this.apiServer.put('/v3/bulk_import/upload_part/test_import/part1').reply(200, {
            bulk_import:"test_import",
            name:"test_import"
        });

        this.apiServer.post('/v3/bulk_import/delete_part/test_import/part1').reply(200, {
            bulk_import:"test_import",
            name:"test_import"
        });

        this.apiServer.post('/v3/bulk_import/perform/test_import').reply(200, {
            name: 'test_import',
            bulk_import: 'test_import',
            job_id: 12345678
        });

        this.apiServer.post('/v3/bulk_import/commit/test_import').reply(200, {
            name: 'test_import',
            bulk_import: 'test_import'
        });
    },

    start: function() {
        this._config();
    }
};



module.exports = TDMockApi;
