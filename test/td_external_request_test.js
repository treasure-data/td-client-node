var assert = require('assert');
var TD = require('../lib/index.js');

var client = new TD(process.env.TREASURE_DATA_API_KEY);

describe.skip('TD with external API', function() {
    describe('First test', function() {
        it('sample', function() {
            assert.equal(1, 2 - 1);
        });
    });

    describe('#listDatabases', function() {
        it('should get all databases', function(done) {
            client.listDatabases(function(err, results) {
                assert.equal(null, err);
                assert(0 < results.databases.length)
                done();
            });
        });
    });

    describe('#listTables', function() {
        it('should get all tables in samples_datasets', function(done) {
            client.listTables('sample_datasets', function(err, results){
                assert.equal(null, err);
                assert(0 < results.tables.length);
                done();
            });
        });
    });

    describe('#createDatabase', function() {
        it('should create td_client_node_test safely', function(done) {
            client.createDatabase('td_client_node_test', function(err, results) {
                assert.equal(null, err);
                done();
            });
        });
    });


    describe('#createLogTable', function() {
        it('should create log table in td_client_node_test', function(done) {
            client.createLogTable('td_client_node_test', 'log_table', function(err, results) {
                assert.equal(null, err);
                assert.equal('log_table', results.table);
                assert.equal('log', results.type);
                done();
            })
        });
    });

    describe('#deleteDatabase', function() {
        it('should delete td_client_node_test safely', function(done) {
            client.deleteDatabase('td_client_node_test', function(err, results) {
                assert.equal(null, err);
                done();
            });
        });
    });

    describe('#listJobs', function() {
        it('should get all jobs', function(done) {
            client.listJobs(function(err, results) {
                assert.equal(null, err);
                assert(null !== results.jobs);
                done();
            });
        });
    });

    describe('#hiveQuery', function() {
        it('submit Hive query', function(done) {
            var q = 'SELECT COUNT (*) FROM www_access';
            client.hiveQuery('sample_datasets', q, function(err, results) {
                assert.equal(null, err);
                assert(null !== results.job);
                done();
            });
        });
    });

    describe('#swapTable', function() {
        // Prepare two tables
        before(function(done) {
            client.createDatabase('td_client_node_test', function(err, results) {
                if (err) return done(err);
                client.createLogTable('td_client_node_test', 'tbl_a', function(err, results) {
                    client.createLogTable('td_client_node_test', 'tbl_b', function(err, results) {
                        if (err) return done(err);
                        done();
                    })
                });
            });
        });

        describe('swap A and B', function() {
            it('should table A and table B safely', function(done) {
                client.swapTable('td_client_node_test', 'tbl_a', 'tbl_b', function(err, results) {
                    assert.equal(null, err);
                    console.log(results);
                    done();
                });
            });
        });

        // Clean previous database
        after(function(done) {
            client.deleteDatabase('td_client_node_test', function(err, results) {
                done();
            });
        });
    });
});
