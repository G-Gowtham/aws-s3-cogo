import boto3
import csv

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


def lifecycle_rule(bucket_name, days_after_deletion):
    s3 = boto3.client('s3')

    try:
        lifecycle = s3.get_bucket_lifecycle(Bucket = bucket_name)
    except ClientError:
        lifecycle = {'Rules': []}

    print(lifecycle)
    tmp_rules = []
    rule_id = ""
    entie_bucket_rules = ""

    for rule in lifecycle['Rules']:
        if 'Prefix' not in rule and 'Status' is 'Enabled':
            rule_id += f'{rule['ID']}+'
        tmp_rules.append(rule)
    
    lifecycle = {'Rules': tmp_rules}

    lifecycle['Rules'].append({
        'ID': 'PruneAbandonedMultipartUploads+hi',
        'Status': 'Enabled',
        'Prefix': "",
        'AbortIncompleteMultipartUpload': {
            'DaysAfterInitiation': days_after_deletion 
        }
    })
    return lifecycle

if __name__ == "__main__":
    s3 = boto3.client('s3')
    file_name = 'accounts-info.csv'
    role_name = 'gowtham-test-role'
    days_after_deletion = 1
    accounts_info = accounts_info(file_name)
    

    for details in accounts_info[1:]:
        account_id = details[1]
        session = assume_role(account_id, role_name)
        bucket_list = list_buckets(session)

        i = 0
        for bucket in bucket_list:
            if i:
                break
            i += 1
            lifecycle = lifecycle_rule(bucket, days_after_deletion)
            s3.put_bucket_lifecycle(Bucket = bucket, LifecycleConfiguration = lifecycle)
