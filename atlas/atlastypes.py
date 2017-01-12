class SparkEtlProcess(object):

    def __init__(self, atlas):
        self.atlas = atlas


    def sparkEtlProcessDef(self):
        sparkEtlProcessJson = {
            'enumTypes': [],
            'structTypes': [],
            'traitTypes': [],
            'classTypes': [ 
                {
                    'hierarchicalMetaTypeName': 'org.apache.atlas.typesystem.types.ClassType',
                    'typeName': 'spark_etl_process',
                    'typeDescription': None,
                    'superTypes': ['Process'],
                    'attributeDefinitions': [
                        {
                            'isUnique': False,
                            'name': 'etl_code',
                            'reverseAttributeName': None,
                            'multiplicity': 'required',
                            'dataTypeName': 'string',
                            'isIndexable': True,
                            'isComposite': False
                        },
                        {
                            'isUnique': False,
                            'name': 'packages',
                            'reverseAttributeName': None,
                            'multiplicity': 'optional',
                            'dataTypeName': 'array<string>',
                            'isIndexable': False,
                            'isComposite': False
                        },
                        {
                            'isUnique': False,
                            'name': 'startTime',
                            'reverseAttributeName': None,
                            'multiplicity': 'required',
                            'dataTypeName': 'date',
                            'isIndexable': False,
                            'isComposite': False
                        },
                        {
                            'isUnique': False,
                            'name': 'endTime',
                            'reverseAttributeName': None,
                            'multiplicity': 'required',
                            'dataTypeName': 'date',
                            'isIndexable': False,
                            'isComposite': False
                        },
                        {
                            'isUnique': False,
                            'name': 'userName',
                            'reverseAttributeName': None,
                            'multiplicity': 'optional',
                            'dataTypeName': 'string',
                            'isIndexable': True,
                            'isComposite': False
                        }
                    ]
                }
            ]
        }
        return sparkEtlProcessJson


    def createOrGetSparkEtlProcess(self):
        sparkEtlProcessType = self.atlas.getType("spark_etl_process")
        if sparkEtlProcessType is None:
            self.atlas.createType(self.sparkEtlProcessDef())
            sparkEtlProcessType = self.atlas.getType("spark_etl_process")
        
        return sparkEtlProcessType

