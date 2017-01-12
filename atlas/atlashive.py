from __future__ import print_function
from .atlasutils import atlasNewGuid


class HiveEntity(object):

    def __init__(self, atlas):
        self.atlas = atlas

    #
    # Entitiy definitions
    #

    def columnJson(self, tableGUID, databaseName, tableName, columnName, columnType, owner):
        columnDef = {
            "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Reference",
            "id": {
              "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Id",
              "id": atlasNewGuid(),
              "version": 0,
              "typeName": "hive_column",
              "state": "ACTIVE"
            },
            "typeName": "hive_column",
            "values": {
              "name": "%s" % columnName,
              "description": None,
              "qualifiedName": "%s.%s.%s@%s" % (databaseName, tableName, columnName, self.atlas.cluster),
              "comment": None,
              "owner": "%s" % owner,
              "type": "%s" % columnType,
              "table": {
                "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Id",
                "id": "%s" % tableGUID,
                "version": 0,
                "typeName": "hive_table",
                "state": "ACTIVE"
              }
            },
            "traitNames": [ ],
            "traits": {  }
        }
        
        return columnDef

        
    def tableJson(self, databaseGuid, databaseName, tableName, createTime, owner):
        tableDef = {
          "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Reference",
          "id": {
            "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Id",
            "id": atlasNewGuid(),
            "version": 0,
            "typeName": "hive_table",
            "state": "ACTIVE"
          },
          "typeName": "hive_table",
          "values": {
            "aliases": None,
            "tableType": "MANAGED_TABLE",
            "name": "%s" % tableName,
            "viewExpandedText": None,
            "createTime": "%s" % createTime,
            "description": None,
            "temporary": False,
            "db": {
              "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Id",
              "id": "%s" % databaseGuid,
              "version": 0,
              "typeName": "hive_db",
              "state": "ACTIVE"
            },
            "viewOriginalText": None,
            "retention": 0,
            "qualifiedName": "%s.%s@%s" % (databaseName, tableName, self.atlas.cluster),
            "columns": [ ],
            "comment": None,
            "lastAccessTime": createTime,
            "owner": "%s" % owner,
            "partitionKeys": None
          },
          "traitNames": [ ],
          "traits": { }
        }

        return tableDef


    def updateTableJson(self, tableGuid, databaseName, tableName, createTime, owner):
        tableDef = {
          "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Reference",
          "id": {
            "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Id",
            "id": "%s" % tableGuid,
            "version": 0,
            "typeName": "hive_table",
            "state": "ACTIVE"
          },
          "typeName": "hive_table",
          "values": {
            "columns": [ ],
          },
          "traitNames": [ ],
          "traits": { }
        }

        return tableDef


    def columnIdJson(self, columnGuid):
        columnIdDef = { "id": {
          "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Id",
          "id": columnGuid,
          "version": 0,
          "typeName": "hive_column",
          "state": "ACTIVE"
        }}
        return columnIdDef


    #
    # Entity access methods
    #

    def getTable(self, dbName, tableName):
        path = "?type=hive_table&property=qualifiedName&value=%s.%s@%s" % (dbName, tableName, self.atlas.cluster)
        return self.atlas.getEntity(path)


    def getDB(self, dbName):
        path = "?type=hive_db&property=qualifiedName&value=%s@%s" % (dbName, self.atlas.cluster)
        return self.atlas.getEntity(path)
        

    def createTable(self, targetDB, targetTable, targetColumns, createTime, owner):
        dbGuid = self.getDB(targetDB)
        assert(dbGuid is not None)

        # Create table without columns

        tableDef = self.tableJson(dbGuid["definition"]["id"]["id"], targetDB, targetTable, createTime, owner)
        newTable = self.atlas.createEntity(tableDef)
        newTableGuid = newTable["entities"]["created"][0]
        print("New Table: %s" % newTableGuid)

        # Create columns

        entities = []
        for column in targetColumns:
            columnName, columnType = column.split(":")
            hiveColumnDef = self.columnJson(newTableGuid, targetDB, targetTable, columnName, columnType, owner)
            entities.append(hiveColumnDef)

        newColumns = self.atlas.createEntity(entities)
        print("New Columns:")
        print(newColumns["entities"]["created"])

        # Add columns to table

        updateTableDef = self.updateTableJson(newTableGuid, targetDB, targetTable, createTime, owner)

        columnIds = [self.columnIdJson(guid)["id"] for guid in newColumns["entities"]["created"]]
        updateTableDef["values"]["columns"] = columnIds

        return self.atlas.updateEntity(newTableGuid, updateTableDef)["entities"]["updated"]
