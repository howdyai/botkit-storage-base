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
const async = require('async'); 

module.exports = function(config) {

    if (!config || !config.storageConnectionString || !config.tablePrefix){
        throw 'You must provide storageConnectionString and tablePrefix in config';
    }

    var tableService = azure.createTableService(config.storageConnectionString);
    var entGen = azure.TableUtilities.entityGenerator;

    function ensureTable(tableName, cb){
        if (ensuredTables[tableName])
        {
            console.log(`Table ${tableName} already ensured`);
            return cb();
        }

        console.log(`Ensuring table: [${tableName}]`);
        tableService.createTableIfNotExists(tableName, (err, result, response) => {
            if (err){
                console.log(`${JSON.stringify(err)}\n${JSON.stringify(result)}\n${JSON.stringify(response)}`);
                throw err;
            }
            ensuredTables[tableName] = true;
            cb();
        });
    }

    var objectsToList = function(cb) {
        return function(err, data) {
            if (err) {
                cb(err, data);
            } else {
                console.log(`ENTRIES ${JSON.stringify(data.entries)}`);
                cb(err, Object.keys(data.entries).map(function(key) {
                    //console.log(`Data ${key} ${JSON.stringify(data.entries[key])}`);
                    return JSON.parse(data.entries[key].Data['_']);
                }));
            }
        };
    };

    var teamTable = `${config.tablePrefix}Teams`;
    var userTable = `${config.tablePrefix}Users`;
    var channelTable = `${config.tablePrefix}Channels`;

    var ensuredTables = {};

    function parseResponse(err, res, cb){
        if (err){
            if (err.code == 'ResourceNotFound'){
                err.displayName = 'NotFound';
                return cb(err, null);
            }
            else{
                throw err;
            }
        }

        cb(err, JSON.parse(res.Data['_']));
    }

    var storage = {
        teams: {
            get: function(team_id, cb) {
                ensureTable(teamTable, (err, result) => {
                    if (err) {throw err;}
                    tableService.retrieveEntity(teamTable,
                        'partition', team_id, (err, res) => {
                            parseResponse(err, res, cb);
                        });
                });
            },
            save: function(team_data, cb) {
                ensureTable(teamTable, (err, result) => {
                    if (err) {throw err;}
                    tableService.insertOrReplaceEntity (teamTable,
                        {
                            PartitionKey: entGen.String('partition'),
                            RowKey: entGen.String(team_data.id),
                            Data: entGen.String(JSON.stringify(team_data))
                        },
                    cb);
                });
            },
            delete: function(team_id, cb) {
                ensureTable(teamTable, (err, result) => {
                    if (err) {throw err;}
                    tableService.deleteEntity(teamTable, {
                        PartitionKey: entGen.String(team_id),
                        RowKey: entGen.String('partition')
                    }, cb);
                });
            },
            all: function(cb) {
                ensureTable(teamTable, (err, result) => {
                    if (err) {throw err;}
                    tableService.queryEntities(teamTable, 
                        new azure.TableQuery(), null, objectsToList(cb));
                });
            }
        },
        users: {
            get: function(user_id, cb) {
                ensureTable(userTable, (err, result) => {
                    if (err) {throw err;}
                    tableService.retrieveEntity(userTable,
                        'partition', user_id, (err, res) => {
                            parseResponse(err, res, cb);
                        });
                });
            },
            save: function(user_data, cb) {
                ensureTable(userTable, (err, result) => {
                    if (err) {throw err;}
                    tableService.insertOrReplaceEntity (userTable,
                        {
                            PartitionKey: entGen.String('partition'),
                            RowKey: entGen.String(user_data.id),
                            Data: entGen.String(JSON.stringify(user_data))
                        },
                    cb);
                });
            },
            delete: function(user_id, cb) {
                ensureTable(userTable, (err, result) => {
                    if (err) {throw err;}
                    tableService.deleteEntity(userTable, {
                        PartitionKey: entGen.String(user_id),
                        RowKey: entGen.String('partition')
                    }, cb);
                });
            },
            all: function(cb) {
                ensureTable(userTable, (err, result) => {
                    if (err) {throw err;}
                    tableService.queryEntities(userTable, 
                        new azure.TableQuery(), null, objectsToList(cb));
                });
            }
        },
        channels: {
            get: function(channel_id, cb) {
                ensureTable(channelTable, (err, result) => {
                    if (err) {throw err;}
                    tableService.retrieveEntity(channelTable,
                        'partition', channel_id, (err, res) => {
                            parseResponse(err, res, cb)
                        });
                });
            },
            save: function(channel_data, cb) {
                ensureTable(channelTable, (err, result) => {
                    if (err) {throw err;}
                    tableService.insertOrReplaceEntity (channelTable,
                        {
                            PartitionKey: entGen.String('partition'),
                            RowKey: entGen.String(channel_data.id),
                            Data: entGen.String(JSON.stringify(channel_data))
                        },
                    cb);
                });
            },
            delete: function(channel_id, cb) {
                ensureTable(channelTable, (err, result) => {
                    if (err) {throw err;}
                    tableService.deleteEntity(channelTable, {
                        PartitionKey: entGen.String(channel_id),
                        RowKey: entGen.String('partition')
                    }, cb);
                });
            },
            all: function(cb) {
                ensureTable(channelTable, (err, result) => {
                    if (err) {throw err;}
                    tableService.queryEntities(channelTable, 
                        new azure.TableQuery(), null, objectsToList(cb));
                });
            }
        }
    };

    return storage;
};