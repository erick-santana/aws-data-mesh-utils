from src.data_mesh_util.DataMeshProducer import DataMeshProducer
import boto3

if __name__ == '__main__':
    mesh_session = boto3.Session(profile_name='MESH')
    producer_session = boto3.Session(profile_name='PRODUCER')
    mesh_account = mesh_session.client('sts').get_caller_identity().get('Account')

    data_mesh_producer = DataMeshProducer(
        data_mesh_account_id=mesh_account,
        use_credentials=producer_session.get_credentials()
    )

    database_name = 'data_mesh'

    data_mesh_producer.create_data_products(
        source_database_name=database_name,
        create_public_metadata=True
    )
