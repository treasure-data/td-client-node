var assert = require('assert');
var TDMockApi = require('./TDMockApi.js');

var TD = require('../lib/index.js');

describe('TD with mock api', function() {

    var client = new TD('MOCK_API_KEY', {
        host: 'mock.api.treasuredata.com'
    });

    before(function() {
        var mockApi = new TDMockApi();
        mockApi.start();
    });

    describe('#getBaseUrl', function() {
        it('should return correct base url', function() {
            var baseUrl = client.baseUrl;
            assert.equal('http://mock.api.treasuredata.com', baseUrl);
        });
    });

    describe('#listDatabases', function() {
        it('should get all databases', function(done) {
            client.listDatabases(function(err, results){
                assert.equal(null, err);
                assert.equal(3, results.databases.length);
                done();
            });
        });
    });

    describe('#deleteDatabase', function() {
       it('should delete target db', function(done) {
          client.deleteDatabase('db_to_be_deleted', function(err, results) {
              assert.equal(null, err);
              assert.equal('db_to_be_deleted', results.database);
              done();
          });
       });
    });

    describe('#createDatabase', function() {
       it('should create target db', function(done) {
          client.createDatabase('db_to_be_created', function(err, results) {
              assert.equal(null, err);
              assert.equal('db_to_be_created', results.database);
              done();
          });
       });
    });

    describe('#listJobs', function() {
        it('should get all jobs', function(done){
            client.listJobs(function(err, results) {
                assert.equal(null, err);
                assert.equal(2, results.count);
                assert.equal(2, results.jobs.length);
                done();
            });
        });
    });

    describe('#showJobs', function() {
        it('should show job detail', function(done) {
            client.showJob('12345', function(err, results) {
                assert.equal(null, err);
                assert.equal(results.type, 'hive');
                assert.equal(results.query, "SELECT * FROM www_access");
                assert.equal(results.job_id, "12345");
                assert.equal(results.status, "success");
                assert.equal(results.url, "http://console.treasuredata.com/job/12345");
                done();
            });
        });
    });

    describe('#listTables', function() {
        it('should get all tables', function(done) {
            client.listTables('my_db', function(err, results) {
                assert.equal(null, err);
                assert.equal('my_db', results.database);
                assert.equal(2, results.tables.length);
                done();
            });
        });
    });

    describe('#swapTable', function() {
        it('should swap two tables', function(done) {
            client.swapTable('my_db', 'tbl1', 'tbl2', function(err, results) {
                assert.equal(null, err);
                assert.equal('my_db', results.database);
                assert.equal('tbl1', results.table1);
                assert.equal('tbl2', results.table2);
                done();
            });
        });
    });

    describe('#hiveQuery', function() {
        it('should submit hive query', function(done) {
            var q = 'SELECT COUNT(*) FROM www_access';
            client.hiveQuery('my_db', q, function(err, results) {
                assert.equal(null ,err);
                assert.equal("12345", results.job_id);
                assert.equal("hive", results.type);
                assert.equal("my_db", results.database);
                assert.equal("http://console.treasure.com/will-be-ready",
                             results.url);
                done();
            });
        });

        it('should submit hive query with options', function(done) {
            var q = 'SELECT COUNT(*) FROM www_access';
            var opts = { result: "web://result.example.com/callback" }

            client.hiveQuery('my_db', q, opts, function(err, results) {
                assert.equal(null ,err);
                assert.equal("12345", results.job_id);
                assert.equal("hive", results.type);
                assert.equal("my_db", results.database);
                assert.equal("http://console.treasure.com/will-be-ready",
                             results.url);
                done();
            });
        });
    });

    describe('#prestoQuery', function() {
       it('should submit presto query', function(done) {
           var q = 'SELECT COUNT(*) FROM www_access';
           client.prestoQuery('my_db', q, function(err, results) {
               assert.equal(null, err);
               assert.equal("12345", results.job);
               assert.equal("my_db", results.database);
               assert.equal("12345", results.job_id);
               done();
           });
       });

       it('should submit presto query with options', function(done) {
           var q = 'SELECT COUNT(*) FROM www_access';
           var opts = { result: "web://result.example.com/callback" }

           client.prestoQuery('my_db', q, opts, function(err, results) {
               assert.equal(null, err);
               assert.equal("12345", results.job);
               assert.equal("my_db", results.database);
               assert.equal("12345", results.job_id);
               done();
           });
       });
    });

    describe('#kill', function() {
        it('should kill target job', function(done) {
            client.kill('12345', function(err, results) {
                assert.equal(null, err);
                assert.equal("12345", results.job_id);
                assert.equal("running", results.former_status);
                done();
            });
        });
    });

    describe('#createSchedule', function() {
        it('should create target schedule', function(done) {
            var opts = {
                cron: '0 * * * *',
                query: 'SELECT COUNT(1) FROM www_access',
                type: 'hive',
                database: 'sample_datasets'
            };
            client.createSchedule('my_scheduled_query', opts, function(err, results) {
                assert.equal(null, err);
                assert.equal('my_scheduled_query', results.name);
                assert.equal('UTC', results.timezone);
                assert.equal(0, results.delay);
                assert.equal('2015-12-08T01:22:27Z', results.created_at);
                assert.equal('2015-12-08T02:00:00Z', results.start);
                assert.equal('hive', results.type);
                assert.equal('SELECT COUNT(1) FROM www_access', results.query);
                assert.equal('sample_datasets', results.database);
                assert.equal('test@example.com', results.user_name);
                done();
            });
        });
    });

    describe('#deleteSchedule', function() {
        it('should delete target schedule', function(done) {
            client.deleteSchedule('my_scheduled_query', function(err, results) {
                assert.equal(null, err);
                assert.equal('my_scheduled_query', results.name);
                assert.equal('Asia/Tokyo', results.timezone);
                assert.equal(0, results.delay);
                assert.equal('2015-12-06T05:43:42Z', results.created_at);
                assert.equal('hive', results.type);
                assert.equal('SELECT COUNT(1) FROM www_access', results.query);
                assert.equal('sample_datasets', results.database);
                assert.equal('test@example.com', results.user_name);
                done();
            });
        });
    });

    describe('#listSchedule', function() {
       it('should list up all scheduled queries', function(done) {
           client.listSchedules(function(err, results) {
               assert.equal(null, err);
               assert.equal(1, results.schedules.length);
               var firstSchedule = results.schedules[0];
               assert.equal('my_scheduled_query', firstSchedule.name);
               assert.equal('0 * * * *', firstSchedule.cron);
               assert.equal('Asia/Tokyo', firstSchedule.timezone);
               assert.equal(0, firstSchedule.delay);
               assert.equal('2015-12-06T05:43:42Z', firstSchedule.created_at);
               assert.equal('hive', firstSchedule.type);
               assert.equal('SELECT COUNT(1) FROM www_access', firstSchedule.query);
               assert.equal('sample_datasets', firstSchedule.database);
               assert.equal('test@example.com', firstSchedule.user_name);
               assert.equal(0, firstSchedule.priority);
               assert.equal(0, firstSchedule.retry_limit);
               assert.equal(null, firstSchedule.next_time);
               done();
           });
       });
    });

    describe('#updateSchedule', function() {
        it('should update target schedule', function(done) {
            var opts = {
                query: 'SELECT COUNT(1) FROM nasdaq'
            };
            client.updateSchedule('my_scheduled_query', opts, function(err, results) {
                assert.equal(null, err);
                assert.equal('my_scheduled_query', results.name);
                assert.equal('UTC', results.timezone);
                assert.equal(0, results.delay);
                assert.equal('2015-12-08T01:22:27Z', results.created_at);
                assert.equal('2015-12-08T02:00:00Z', results.start);
                assert.equal('hive', results.type);
                assert.equal('SELECT COUNT(1) FROM nasdaq', results.query);
                assert.equal('sample_datasets', results.database);
                assert.equal('test@example.com', results.user_name);
                done();
            });
        });
    });

    describe('#runSchedule', function() {
        it('should return job record ', function(done) {
            client.runSchedule('12345', '1448928000', function(err, results) {
                assert.equal(null, err);
                assert.equal(1, results.jobs.length);
                done();
            });
        });
    });

    describe('Custome Header with show job', function() {
        it('should send custom header', function(done) {
            var customHeaderClient = new TD('MOCK_API_KEY', {
                host: 'mock.api.treasuredata.com',
                headers: {
                    'Custom Header': 'CUSTOM'
                }
            });

            customHeaderClient.showJob('custom_header', function(err, results) {
                assert.equal(null, err);
                done();
            });
        });

    })
});
