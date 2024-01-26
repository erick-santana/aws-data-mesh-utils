from src.data_mesh_util.DataMeshProducer import DataMeshProducer
import boto3

if __name__ == '__main__':
    mesh_session = boto3.Session(profile_name='MESH')
    producer_session = boto3.Session(profile_name='PRODUCER')
    consumer_session = boto3.Session(profile_name='CONSUMER')

    mesh_account = mesh_session.client('sts').get_caller_identity().get('Account')
    producer_account = producer_session.client('sts').get_caller_identity().get('Account')

    data_mesh_producer = DataMeshProducer(
        data_mesh_account_id=mesh_account,
        region_name=mesh_session.region_name,
        use_credentials=producer_session.get_credentials()
    )

    # get the pending access requests
    pending_requests = data_mesh_producer.list_pending_access_requests()

    # pick one to approve
    choose_subscription = pending_requests.get('Subscriptions')[0]

    # The subscription ID that the Consumer created and returned from list_pending_access_requests()
    subscription_id = choose_subscription.get('SubscriptionId')

    # Set the permissions to grant to the Consumer - in this case whatever they asked for
    grant_permissions = choose_subscription.get('RequestedGrants')

    # List of permissions the consumer can pass on. Usally only DESCRIBE or SELECT
    grantable_permissions = ['DESCRIBE','SELECT']

    # String value to associate with the approval
    approval_notes = 'Enjoy!'

    # approve access requested
    approval = data_mesh_producer.approve_access_request(
        request_id=subscription_id,
        grant_permissions=grant_permissions,
        grantable_permissions=grantable_permissions,
        decision_notes=approval_notes
    )

    # or deny access request
    # approval = data_mesh_producer.deny_access_request(
    #     request_id=subscription_id,
    #     decision_notes="no way"
    # )