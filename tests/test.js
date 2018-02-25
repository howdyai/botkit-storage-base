/*
Tests for storage modules.
This file currently test simple_storage.js, redis_storage, and firebase_storage.

If you build a new storage module,
you must add it to this test file before your PR will be considered.
How to add it to this test file:

Add the following to the bottom of this file:

// Test <your_storage_module>
<your_storage_module> = require('./<your_storage_module>.js')(<appropriate config object for your storage module>);
check(<your_storage_module>.users);
check(<your_storage_module>.channels);
check(<your_storage_module>.teams);
*/

var test = require('unit.js');
const azure = require('azure-storage');

testObj0 = {id: 'TEST0', foo: 'bar0'};
testObj1 = {id: 'TEST1', foo: 'bar1'};

var testStorageMethod = function(storageMethod) {
    storageMethod.save(testObj0, function(err) {
        if (err){console.error(err);}
        test.assert(!err);
        storageMethod.save(testObj1, function(err) {
            test.assert(!err);
            storageMethod.get(testObj0.id, function(err, data) {
                test.assert(!err);
                console.log(data);
                test.assert(data.foo === testObj0.foo);
            });
            storageMethod.get('shouldnt-be-here', function(err, data) {
                test.assert(err.displayName === 'NotFound');
                test.assert(!data);
            });
            storageMethod.all(function(err, data) {
                test.assert(!err);
                console.log(data);
                test.assert(
                    data[0].foo === testObj0.foo && data[1].foo === testObj1.foo ||
                    data[0].foo === testObj1.foo && data[1].foo === testObj0.foo
                );
            });
        });
    });
};

console.log('If no asserts failed then the test has passed!');

// Test azure storage
var azure_storage = require('../src/index.js')({
    tablePrefix: 'botkit',
    storageConnectionString: 'UseDevelopmentStorage=true'
});
testStorageMethod(azure_storage.users);
testStorageMethod(azure_storage.channels);
testStorageMethod(azure_storage.teams);