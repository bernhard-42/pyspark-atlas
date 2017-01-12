from datetime import datetime
import time
import inspect
from .atlashive import HiveEntity
from .atlaslineage import Lineage
from .atlashttp import AtlasHttp
from .atlastypes import SparkEtlProcess

class Atlas(object):

    def __init__(self, cluster, host, user, password):
        self.cluster = cluster
        self.http = AtlasHttp(host, user, password)
        self.lineage = Lineage(self)
        self.hive = HiveEntity(self)
        self.spark = SparkEtlProcess(self)


    def now(self):
        return datetime.utcnow().isoformat()[:23] + "Z"


    def timestamp(self, utc):
        return utc.isoformat()[:23] + "Z"

        
    def createType(self, content):
        return self.http.POST("types", content)


    def getType(self, qualifiedName):
        return self.http.GET("types/%s" % qualifiedName)


    def createEntity(self, content):
        return self.http.POST("entities", content)


    def getEntity(self, path):
        return self.http.GET("entities/%s" % path)


    def updateEntity(self, guid, content):
        return self.http.POST("entities/%s" % guid, content)


    def deleteEntity(self, guid):
        return self.http.DELETE("entities?guid=%s" % guid)
