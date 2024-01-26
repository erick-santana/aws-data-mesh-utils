import boto3
import csv

if __name__ == '__main__':
    producer_session = boto3.Session(profile_name='PRODUCER')
    producer_account_id = producer_session.client('sts').get_caller_identity().get('Account')

    file_path = 'data.csv'

    data = [
        [1, 'John', 25],
        [2, 'Alice', 30],
        [3, 'Bob', 35],
        [4, 'Charlie', 40],
        [5, 'Emma', 45],
        [6, 'Oliver', 50],
        [7, 'Sophia', 55],
        [8, 'James', 60],
        [9, 'Mia', 65],
        [10, 'William', 70]
    ]

    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(data)

    print(f"CSV file created at {file_path}")

    s3 = producer_session.client('s3')

    print("Creating bucket...")
    s3.create_bucket(Bucket=f'data-mesh-{producer_account_id}-us-east-1')

    print("Uploading file...")
    s3.upload_file(file_path, f'data-mesh-{producer_account_id}-us-east-1', 'data_mesh_table/data.csv')

    print("File uploaded to s3")
