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

    var teamTable = `${config.tablePrefix}Teams`;
    var userTable = `${config.tablePrefix}Users`;
    var channelTable = `${config.tablePrefix}Channels`;

    var ensuredTables = {};

    function retrieveEntity(table, id, cb) {
        ensureTable(table, (err, res) => {
            if (err){throw err;}
            tableService.retrieveEntity(table, 'partition', id, (err, res) => {
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
            });
        });
    }

    function insertEntity(table, entity, cb){
        ensureTable(table, (err, result) => {
            if (err) {throw err;}
            tableService.insertOrReplaceEntity (table,
                {
                    PartitionKey: entGen.String('partition'),
                    RowKey: entGen.String(entity.id),
                    Data: entGen.String(JSON.stringify(entity))
                },
            cb);
        });
    }

    function deleteEntity(table, id, cb){
        ensureTable(table, (err, result) => {
            if (err) {throw err;}
            tableService.deleteEntity(table, {
                PartitionKey: entGen.String(id),
                RowKey: entGen.String('partition')
            }, cb);
        });
    }

    function allEntities(table, cb){ 
        ensureTable(teamTable, (err, result) => {
            if (err) {throw err;}
            tableService.queryEntities(table, 
                new azure.TableQuery(), null, (err, data) => { 
                    cb(err, Object.keys(data.entries).map(function(key) {
                        return JSON.parse(data.entries[key].Data['_']);
                    }));
                });
        });
    }

    var storage = {
        teams: {
            get: function(team_id, cb) {
                retrieveEntity(teamTable, team_id, cb);
            },
            save: function(team_data, cb) {
                insertEntity(teamTable, team_data, cb);
            },
            delete: function(team_id, cb) {
                deleteEntity(teamTable, team_id, cb);
            },
            all: function(cb) {
                allEntities(teamTable, cb);
            }
        },
        users: {
            get: function(user_id, cb) {
                retrieveEntity(userTable, user_id, cb);
            },
            save: function(user_data, cb) {
                insertEntity(userTable, user_data, cb);
            },
            delete: function(user_id, cb) {
                deleteEntity(userTable, user_id, cb);
            },
            all: function(cb) {
                allEntities(userTable, cb);
            }
        },
        channels: {
            get: function(channel_id, cb) {
                retrieveEntity(channelTable, channel_id, cb);
            },
            save: function(channel_data, cb) {
                insertEntity(channelTable, channel_data, cb);
            },
            delete: function(channel_id, cb) {
                deleteEntity(channelTable, channel_id, cb);
            },
            all: function(cb) {
                allEntities(channelTable, cb);
            }
        }
    };

    return storage;
};