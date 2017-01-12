from __future__ import print_function
import time
from datetime import datetime

from pyspark import SparkContext
from pyspark.sql import HiveContext

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, concat

from atlas import Atlas

#
# Atlas Config
#
cluster   = "Sandbox"
atlasHost = "localhost:21000"
user      = "holger_gov"
password  = "holger_gov"


#
# (1) Metadata for the etl job
#
sourceDB          = "employees"
sourceTables      = ("employees", "departments", "dept_emp")

targetDB          = "default"
targetTable       = "emp_dept_flat"
targetColumns     = ["dept_no:string", "emp_no:int", "full_name:string", "from_date:string", "to_date:string", "dept_name:string"]

usedSparkPackages = []
owner = "etl"


#
# (2) This is the actual ETL Spark function and its source will be copied into ATLAS Lineage
#
def etl():
    def saveToHiveAsORC(df, database, table):
        tempTable = "%s_tmp_%d" % (table, (time.time()))
        df.registerTempTable(tempTable)
        sqlContext.sql("create table %s.%s stored as ORC as select * from %s" % (database, table, tempTable))
        sqlContext.dropTempTable(tempTable)

    employees   = sqlContext.sql("select * from %s.%s" % (sourceDB, sourceTables[0]))  # -> employees.employees
    departments = sqlContext.sql("select * from %s.%s" % (sourceDB, sourceTables[1]))  # -> employees.departments
    dept_emp    = sqlContext.sql("select * from %s.%s" % (sourceDB, sourceTables[2]))  # -> employees.dept_emp

    emp_dept_flat = employees.withColumn("full_name", concat(employees["last_name"], lit(", "), employees["first_name"]))
    emp_dept_flat = emp_dept_flat.select("full_name", "emp_no").join(dept_emp,"emp_no").join(departments, "dept_no")
    saveToHiveAsORC(emp_dept_flat, targetDB, targetTable)

    return emp_dept_flat


#
# Execute the ETL job in Spark ...
#
print("==> Executing Spark ETL job ")

master = "local[*]"
appName = "Simple ETL Job"
sc = SparkContext(master, appName)

sc.setLogLevel("ERROR")

sqlContext = HiveContext(sc)

startTime = datetime.utcnow()
emp_dept_flat = etl()
endTime = datetime.utcnow()
print("Started: %s" % startTime.isoformat())
print("Ended:   %s" % endTime.isoformat())


#
# ... and add the lineage to Atlas
#  
print("Adding Lineage to Atlas (using the code in function 'etl')")

atlas = Atlas(cluster, atlasHost, user, password)
atlas.lineage.createHiveToHive(sourceDB, sourceTables, targetDB, targetTable, targetColumns, \
                               etl, usedSparkPackages, atlas.timestamp(startTime), atlas.timestamp(endTime), owner)

print("Done")