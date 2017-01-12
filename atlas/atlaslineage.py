from datetime import datetime
import inspect
from .atlasutils import atlasNewGuid


class Lineage(object):

    def __init__(self, atlas):
        self.atlas = atlas
        self.inputGuids = {"dbs": {}, "tables": {}}
        self.outputTableGuid = ""


    def lineageJson(self, inputTableNames, inputTableGuids, outputTableName, outputTableGuid, 
                          sparkCode, packages, startTime, endTime, owner):
        name = "Spark:%s->%s" %(",".join(inputTableNames), outputTableName)
        lineageDef = {
            "id": {
                "id": atlasNewGuid(),
                "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Id",
                "state": "ACTIVE",
                "typeName": "spark_etl_process",
                "version": 0
            },
            "typeName": "spark_etl_process",
            "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Reference",
            "values": {
                "name": name,
                "qualifiedName": "%s@%s" %(name, self.atlas.cluster),
                "inputs": [],
                "outputs": [
                    {
                        "id": "%s" % outputTableGuid,
                        "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Id",
                        "state": "ACTIVE",
                        "typeName": "hive_table",
                        "version": 0
                    }
                ],
                "etl_code": "%s"   % sparkCode,
                "query_text": "%s" % sparkCode,
                "packages": "%s"   % packages,
                "startTime": "%s"  % startTime,
                "endTime": "%s"    % endTime,
                "userName": "%s"   % owner
            },
            "traitNames": [],
            "traits": {}
        }
        
        for guid in inputTableGuids:
            tableDef = {
                "id": "%s" % guid,
                "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Id",
                "state": "ACTIVE",
                "typeName": "hive_table",
                "version": 0
            }
            lineageDef["values"]["inputs"].append(tableDef)
        
        return lineageDef


    def getTableGuids(self, sourceDB, sourceTables, targetDB, targetTable):
        inputGuids = []

        for table in sourceTables:
            tableDef = self.atlas.hive.getTable(sourceDB, table)
            inputGuids.append(tableDef["definition"]["id"]["id"])
        
        tableDef = self.atlas.hive.getTable(targetDB, targetTable)
        outputGuid = tableDef["definition"]["id"]["id"]
        
        return (outputGuid, inputGuids)


    def createLineage(self, inputTableNames, inputTableGuids, outputTableName, outputTableGuid, etlFunc, packages, startTime, endTime, owner):
        transformation = """<pre>%s</pre>""" % inspect.getsource(etlFunc)
        lineageDef = self.lineageJson(inputTableNames, inputTableGuids, outputTableName, outputTableGuid, transformation, packages, startTime, endTime, owner)
        newLineage = self.atlas.createEntity(lineageDef)
        
        return newLineage["entities"]["created"]


    def createHiveToHive(self, sourceDB, sourceTables, targetDB, targetTable, targetColumns, etlFunc, packages, startTime, endTime, owner):
        tableRef = self.atlas.hive.createTable(targetDB, targetTable, targetColumns, endTime, owner)
        outputGuid, inputGuids = self.getTableGuids(sourceDB, sourceTables, targetDB, targetTable)
        
        lineageRef = self.createLineage(sourceTables, inputGuids, targetTable, outputGuid, etlFunc, packages, startTime, endTime, owner)
        print("New Lineage:")
        print(lineageRef)

        return lineageRef

