import boto3
import asyncio
import aioboto3


s3 = boto3.client('s3')

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


