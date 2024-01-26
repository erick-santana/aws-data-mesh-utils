from src.data_mesh_util.DataMeshAdmin import DataMeshAdmin
import logging
import boto3

if __name__ == '__main__':
    mesh_session = boto3.Session(profile_name='MESH')

    mesh_admin = DataMeshAdmin(
        data_mesh_account_id=mesh_session.client('sts').get_caller_identity().get('Account'),
        region_name=mesh_session.region_name,
        log_level=logging.DEBUG,
        use_credentials=mesh_session.get_credentials()
    )

    mesh_admin.initialize_mesh_account()
