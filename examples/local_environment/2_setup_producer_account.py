from src.data_mesh_util.DataMeshMacros import DataMeshMacros
from src.data_mesh_util.lib.constants import PRODUCER
import logging
import boto3

if __name__ == '__main__':
    mesh_session = boto3.Session(profile_name='MESH')
    producer_session = boto3.Session(profile_name='PRODUCER')

    mesh_macros = DataMeshMacros(
        data_mesh_account_id=mesh_session.client('sts').get_caller_identity().get('Account'),
        region_name=mesh_session.region_name,
        log_level=logging.DEBUG
    )

    mesh_macros.bootstrap_account(
        account_type=PRODUCER,
        mesh_credentials=mesh_session.get_credentials(),
        account_credentials=producer_session.get_credentials()
    )