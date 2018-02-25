/*
Storage module for bots.

Supports storage of data on a team-by-team, user-by-user, and chnnel-by-channel basis.

save can be used to store arbitrary object.
These objects must include an id by which they can be looked up.
It is recommended to use the team/user/channel id for this purpose.
Example usage of save:
controller.storage.teams.save({id: message.team, foo:"bar"}, function(err){
  if (err)
    console.log(err)
});

get looks up an object by id.
Example usage of get:
controller.storage.teams.get(message.team, function(err, team_data){
  if (err)
    console.log(err)
  else
    console.log(team_data)
});
*/

const azure = require('azure-storage');

module.exports = function(config) {

    if (!config || !config.storageConnectionString || !config.tablePrefix){
        throw 'You must provide storageConnectionString and tablePrefix in config';
    }

    var tableService = azure.createTableService(config.storageConnectionString);
    var entGen = azure.TableUtilities.entityGenerator;

    var teamTable = `${config.tablePrefix}Teams`;
    var userTable = `${config.tablePrefix}Users`;
    var channelTable = `${config.tablePrefix}Channels`;

    var ensuredTables = {};

    function cb2Promise(resolve, reject){
        return function(err, result){
            if (err){return reject(err);}
                resolve(result);
        }
    }

    function createTableIfNotExistsPromise(tableName){
        return new Promise((resolve, reject) => {
            tableService.createTableIfNotExists(tableName, cb2Promise(resolve,reject));
        });
    }

    function retrieveEntityPromise(table, id){
        return new Promise((resolve, reject) => {
            tableService.retrieveEntity(table, 'partition', id, cb2Promise(resolve,reject));
        });
    }
    function insertOrReplaceEntityPromise(table, entity){
        return new Promise((resolve, reject) => {
            tableService.insertOrReplaceEntity(table, entity, cb2Promise(resolve,reject));
        });
    }
    function deleteEntityPromise(table, entity){
        return new Promise((resolve, reject) => {
            tableService.deleteEntity(table, entity, cb2Promise(resolve,reject));
        });
    }

    function queryEntitiesPromise(table, query){
        return new Promise((resolve, reject)=>{
            tableService.queryEntities(table, query, null, cb2Promise(resolve,reject));
        });
    }

    function ensureTable(tableName){
        return createTableIfNotExistsPromise(tableName)
        .then(value =>
            ensuredTables[tableName] = true
        );
    }

    function retrieveEntity(table, id) {
        return ensureTable(table)
        .then(value => 
            retrieveEntityPromise(table, id)
            .then(value =>
                JSON.parse(value.Data['_']))
            .catch(e => {
                if (e.code == 'ResourceNotFound'){
                    e.displayName = 'NotFound';
                }
                throw e;
            }
        ));
    }

    function insertEntity(table, entity){
        return ensureTable(table)
        .then(value =>
            insertOrReplaceEntityPromise(table,
                {
                    PartitionKey: entGen.String('partition'),
                    RowKey: entGen.String(entity.id),
                    Data: entGen.String(JSON.stringify(entity))
                })
        );
    }

    function deleteEntity(table, id, cb){
        return ensureTable(table)
        .then(value =>
            deleteEntityPromise(table, {
                PartitionKey: entGen.String(id),
                RowKey: entGen.String('partition')
            })
        );
    }

    function allEntities(table){ 
        return ensureTable(table)
        .then(value =>
            queryEntitiesPromise(table, new azure.TableQuery(), null)
            .then(data => Object.keys(data.entries).map(function(key) {
                return JSON.parse(data.entries[key].Data['_']);
            }))
        );
    }

    function nodeify(promise, cb){
        promise.then(value =>
            cb(null, value),
            error => cb(error)
        ).catch(error => {
            console.error(`Error in callback ${error}`);
        });
    }

    var storage = {
        teams: {
            get: function(team_id, cb) {
                nodeify(retrieveEntity(teamTable, team_id), cb);
            },
            save: function(team_data, cb) {
                nodeify(insertEntity(teamTable, team_data), cb);
            },
            delete: function(team_id, cb) {
                nodeify(deleteEntity(teamTable, team_id), cb);
            },
            all: function(cb) {
                nodeify(allEntities(teamTable), cb);
            }
        },
        users: {
            get: function(user_id, cb) {
                nodeify(retrieveEntity(userTable, user_id), cb);
            },
            save: function(user_data, cb) {
                nodeify(insertEntity(userTable, user_data), cb);
            },
            delete: function(user_id, cb) {
                nodeify(deleteEntity(userTable, user_id), cb);
            },
            all: function(cb) {
                nodeify(allEntities(userTable), cb);
            }
        },
        channels: {
            get: function(channel_id, cb) {
                nodeify(retrieveEntity(channelTable, channel_id), cb);
            },
            save: function(channel_data, cb) {
                nodeify(insertEntity(channelTable, channel_data), cb);
            },
            delete: function(channel_id, cb) {
                nodeify(deleteEntity(channelTable, channel_id), cb);
            },
            all: function(cb) {
                nodeify(allEntities(channelTable), cb);
            }
        }
    };

    return storage;
};