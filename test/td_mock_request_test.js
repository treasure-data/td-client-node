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

    describe('#runSchedule', function() {
        it('should return job record ', function(done) {
            client.runSchedule('12345', '1448928000', function(err, results) {
                assert.equal(null, err);
                assert.equal(1, results.jobs.length);
                done();
            });
        });
    });
});
