var assert = require('assert');
var TD = require('../lib/index.js');

var client = new TD(process.env.TREASURE_DATA_API_KEY);

describe('TD with external API', function() {
    describe('First test', function() {
        it('sample', function() {
            assert.equal(1, 2 - 1);
        });
    });

    describe('#listDatabases', function() {
        it.skip('should get all databases', function(done) {
            client.listDatabases(function(err, results) {
                assert.equal(null, err);
                assert(0 < results.databases.length)
                done();
            });
        });
    });

    describe('#listTables', function() {
        it.skip('should get all tables in samples_datasets', function(done) {
            client.listTables('sample_datasets', function(err, results){
                assert.equal(null, err);
                assert(0 < results.tables.length);
                done();
            });
        });
    });
});
