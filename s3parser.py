import boto3
import asyncio
import aioboto3
import json

s3 = boto3.client('s3')
iam = boto3.client('iam')


def role_exists(role_name):
    try:
        iam.get_role(RoleName=role_name)
        return True
    except iam.exceptions.NoSuchEntityException:
        return False


def create_iam_role_and_attach_to_bucket(bucket_name):
    role_name = 'dbMonitor1'
    role_response = None  # Initialize the role_response variable
    if not role_exists(role_name):
        # Create an IAM role with desired permissions
        trust_policy = {
            'Version': '2012-10-17',
            'Statement': [{
                'Effect': 'Allow',
                'Principal': {
                    'Service': 's3.amazonaws.com'
                },
                'Action': 'sts:AssumeRole'
            }]
        }
        role_response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy)
        )

        # Attach an S3 policy to the IAM role
        s3_policy = {
            'Version': '2012-10-17',
            'Statement': [{
                'Effect': 'Allow',
                'Action': [
                    's3:GetObject',
                    's3:PutObject',
                    's3:ListBucket'
                ],
                'Resource': [
                    f'arn:aws:s3:::{bucket_name}/*',
                    f'arn:aws:s3:::{bucket_name}'
                ]
            }]
        }
        iam.put_role_policy(
            RoleName=role_name,
            PolicyName='S3AccessPolicy',
            PolicyDocument=json.dumps(s3_policy)
        )

        print(f"IAM role {role_name} created.")

    # Attach the role to the S3 bucket
    if role_response:
        bucket_policy = {
            'Version': '2012-10-17',
            'Statement': [{
                'Effect': 'Allow',
                'Principal': {
                    'AWS': role_response['Role']['Arn']
                },
                'Action': 's3:*',
                'Resource': f'arn:aws:s3:::{bucket_name}/*'
            }]
        }
        s3.put_bucket_policy(
            Bucket=bucket_name,
            Policy=json.dumps(bucket_policy)
        )

        print(f"IAM role {role_name} attached to {bucket_name} bucket.")

def traverseS3Objects(url):
    bucket_name = url.split('.s3')[0].replace('https://', '')
    object_key = url.split('.com/')[1]

    response = s3.list_objects_v2(
        Bucket=bucket_name, Prefix=object_key)
    paths = response['Contents']
    while len(paths) > 1:
        print("File Paths Display (Choose one of those by number): >>>\n=======================")
        for i, obj in enumerate(paths):
            print(f"{i}: {obj['Key']}")
        path_num = -1
        while path_num < 0 or path_num >= len(paths):
            try:
                path_num = int(input("Path Number: "))
            except:
                print(f"Choose a valid number between 0 and {len(paths)-1}")
        response = s3.list_objects_v2(
            Bucket=bucket_name, Prefix=paths[path_num]['Key'])
        paths = response['Contents']
    file_path = paths[0]['Key']
    return (bucket_name, file_path)


def getS3Data(chunk_size, bucket_name, file_path):
    print(f"Bucket: {bucket_name}, File path: {file_path}\n")
    response = s3.get_object(Bucket=bucket_name, Key=file_path)
    data = response['Body']#.read().decode('utf-8')
    if chunk_size:
        data_chunks = []  # Initialize a list to hold the data chunks
        while True:  # Read the data in chunks
            chunk = data.readline().decode('utf-8')
            if not chunk: break
            data_chunks.append(chunk)
            if len(data_chunks) == chunk_size: # Stop reading after reaching the desired number of lines
                break
        data_chunks = "".join(data_chunks)
        return data_chunks
    
    #If no chunk size was specified, return the whole data
    return data.read().decode('utf-8')
    
def read_data_chunks(data, chunk_size):
    data_chunks = []
    while len(data_chunks) < chunk_size:
        chunk = data.readline().decode('utf-8')
        if not chunk:
            break
        data_chunks.append(chunk)
    return "".join(data_chunks)


def read_whole_data(data):
    return data.read().decode('utf-8')


def monitor_s3_file(s3, bucket_name, file_path):
    print(f"Monitoring on path: {bucket_name}, {file_path}")
    response = s3.get_object(Bucket=bucket_name, Key=file_path)
    data = response['Body']
    prev = read_whole_data(data)

    while True:
        response = s3.get_object(Bucket=bucket_name, Key=file_path)
        data = response['Body']
        curr = read_whole_data(data)
        prev_lines, curr_lines = prev.split('\n'), curr.split('\n')
        rows_curr, rows_prev = len(curr_lines), len(prev_lines)
        if rows_curr == rows_prev:                                                              # Change detected in the rows but no addition or deletion
            for i in range(rows_curr):
                if curr_lines[i] == prev_lines[i]: continue
                print(f"Modified Entry {i+1}: {prev_lines[i]} ===> {curr_lines[i]}")
        else:
            if rows_curr > rows_prev:
                for line in curr_lines:
                    if line not in prev_lines:
                        print(f"New Entry: {line}")  
            else:
                for line in prev_lines:
                    if line not in curr_lines:
                        print(f"Deleted Entry: {line}")
        prev = curr


