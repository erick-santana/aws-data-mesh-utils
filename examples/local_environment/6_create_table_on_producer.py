import boto3

if __name__ == '__main__':
    producer_session = boto3.Session(profile_name='PRODUCER')
    producer_account_id = producer_session.client('sts').get_caller_identity().get('Account')
    glue_client = producer_session.client('glue')

    print("Creating table...")
    glue_client.create_table(
        CatalogId=producer_account_id,
        DatabaseName='data_mesh',
        TableInput={
            'Name': 'data_mesh_table',
            'Description': 'Data Mesh Table',
            'StorageDescriptor': {
                'Columns': [
                    {
                        'Name': 'id',
                        'Type': 'int',
                        'Comment': 'id'
                    },
                    {
                        'Name': 'name',
                        'Type': 'string',
                        'Comment': 'name'
                    },
                    {
                        'Name': 'age',
                        'Type': 'int',
                        'Comment': 'age'
                    }
                ],
                'Location': f's3://data-mesh-{producer_account_id}-us-east-1/data_mesh_table/',
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'Compressed': False,
                'NumberOfBuckets': -1,
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                    'Parameters': {
                        'field.delim': ',',
                        'serialization.format': ','
                    }
                },
                'BucketColumns': [],
                'SortColumns': [],
                'Parameters': {
                    'CrawlerSchemaDeserializerVersion': '1.0',
                    'CrawlerSchemaSerializerVersion': '1.0',
                    'UPDATED_BY_CRAWLER': 'data-mesh',
                    'averageRecordSize': '15',
                    'classification': 'csv',
                    'compressionType': 'none',
                    'objectCount': '1',
                    'recordCount': '1',
                    'sizeKey': '15',
                    'typeOfData': 'file'
                },
                'StoredAsSubDirectories': False
            },
            'PartitionKeys': [],
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {
                'classification': 'csv',
                'compressionType': 'none',
                'typeOfData': 'file'
            }
        }
    )

    print("Table created!")
