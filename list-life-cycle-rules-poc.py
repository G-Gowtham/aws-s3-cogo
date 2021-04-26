import boto3
import csv
from botocore.exceptions import ClientError

def accounts_info(file_name):
    with open(file_name) as f:
        reader = csv.reader(f)
        accounts_info = list(reader)
    
    return accounts_info

def list_buckets(session):
    s3 = session.client('s3')
    response = s3.list_buckets()
    bucket_list = []
    for bucket in response['Buckets']:
        bucket_list.append(bucket["Name"])

    return bucket_list

def assume_role(account_id, role_name):
    role_info = {
    'RoleArn': f'arn:aws:iam::{account_id}:role/{role_name}',
    'RoleSessionName': f'{account_id}_session'
    } 

    client = boto3.client('sts')
    credentials = client.assume_role(**role_info)

    session = boto3.session.Session(
        aws_access_key_id=credentials['Credentials']['AccessKeyId'],
        aws_secret_access_key=credentials['Credentials']['SecretAccessKey'],
        aws_session_token=credentials['Credentials']['SessionToken']
    )
    return session


def lifecycle_rule(account_id, bucket_name, days_after_deletion):
    s3 = boto3.client('s3')

    try:
        lifecycle = s3.get_bucket_lifecycle(Bucket = bucket_name)
    except ClientError:
        lifecycle = {'Rules': []}

    print(lifecycle)


    for rule in lifecycle['Rules']:
        with open('lifecycle-rules.csv', 'a') as f:
            f.write(f'{account_id},{bucket_name},'+rule['ID']+f',{rule}\n')
            

    
    return lifecycle

if __name__ == "__main__":
    s3 = boto3.client('s3')
    file_name = 'accounts-info.csv'
    role_name = 'gowtham-test-role'
    days_after_deletion = 1
    accounts_info = accounts_info(file_name)
    

    with open('lifecycle-rules.csv', 'w') as f:
        f.writelines('Account-id,Bucket Name,Rule,Properties\n')

    for details in accounts_info[1:]:
        account_id = details[1]
        session = assume_role(account_id, role_name)
        bucket_list = list_buckets(session)

        for bucket in bucket_list:
            lifecycle = lifecycle_rule(account_id, bucket, days_after_deletion)
