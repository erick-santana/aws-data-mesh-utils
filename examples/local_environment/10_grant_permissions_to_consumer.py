from src.data_mesh_util.DataMeshConsumer import DataMeshConsumer
import boto3

if __name__ == '__main__':
    mesh_session = boto3.Session(profile_name='MESH')
    consumer_session = boto3.Session(profile_name='CONSUMER')

    mesh_account = mesh_session.client('sts').get_caller_identity().get('Account')

    data_mesh_consumer = DataMeshConsumer(
        data_mesh_account_id=mesh_account,
        region_name=mesh_session.region_name,
        use_credentials=consumer_session.get_credentials()
    )

    # use the subscription ID which has been requested in step 8
    subscription_id = ''

    data_mesh_consumer.finalize_subscription(subscription_id=subscription_id)
