from src.data_mesh_util.DataMeshConsumer import DataMeshConsumer
import boto3

if __name__ == '__main__':
    mesh_session = boto3.Session(profile_name='MESH')
    producer_session = boto3.Session(profile_name='PRODUCER')
    consumer_session = boto3.Session(profile_name='CONSUMER')

    mesh_account = mesh_session.client('sts').get_caller_identity().get('Account')
    producer_account = producer_session.client('sts').get_caller_identity().get('Account')

    data_mesh_consumer = DataMeshConsumer(
        data_mesh_account_id=mesh_account,
        region_name=mesh_session.region_name,
        use_credentials=consumer_session.get_credentials()
    )

    owner_account_id = producer_account
    database_name = f'data_mesh-{producer_account}'
    tables = 'data_mesh_table'
    request_permissions = ['SELECT', 'DESCRIBE']

    subscription = data_mesh_consumer.request_access_to_product(
        owner_account_id=owner_account_id,
        database_name=database_name,
        tables=tables,
        request_permissions=request_permissions
    )

    # SubscriptionId is needed to finalize access in step 10
    print(subscription.get('SubscriptionId'))
