import boto3

if __name__ == '__main__':
    producer_session = boto3.Session(profile_name='PRODUCER')

    glue_client = producer_session.client('glue')

    print("Creating database...")
    db_name = 'data_mesh'
    glue_client.create_database(
        DatabaseInput={
            'Name': db_name
        }
    )

    print(f"Database {db_name} created!")
