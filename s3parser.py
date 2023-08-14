import boto3
import asyncio
import aioboto3


s3 = boto3.client('s3')
pipe_name = r'\\.\pipe\my_pipe'

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


def getS3Data(url, chunk_size):
    bucket_name, file_path = traverseS3Objects(url)
    response = s3.get_object(Bucket=bucket_name, Key=file_path)
    data = response['Body']#.read().decode('utf-8')
    if chunk_size:
        data_chunks = []  # Initialize a list to hold the data chunks
        while True:  # Read the data in chunks
            chunk = data.readline().decode('utf-8')
            if not chunk:
                break
            data_chunks.append(chunk)
            if len(data_chunks) == chunk_size: # Stop reading after reaching the desired number of lines
                break
        data_chunks = "".join(data_chunks)
        return data_chunks
    
    #If no chunk size was specified, return the whole data
    return data.read().decode('utf-8')
    

async def monitorS3(url, chunk_size):
    #Make a new pipe for the monitoring window
    with open(pipe_name, 'w') as pipe:
        # Get the first set of Data Chunks as a base for monitoring
        bucket_name, file_path = traverseS3Objects(url)
        response = s3.get_object(Bucket=bucket_name, Key=file_path)
        data = response['Body']
        data_chunks = []

        while True:
            chunk = data.readline().decode('utf-8')
            if not chunk:
                break
            data_chunks.append(chunk)
            if len(data_chunks) == chunk_size:
                break
        prev = "".join(data_chunks)

        while True:
            response = s3.get_object(Bucket=bucket_name, Key=file_path)
            data = response['Body']
            data_chunks = []

            while True:
                chunk = data.readline().decode('utf-8')
                if not chunk:
                    break
                data_chunks.append(chunk)
                if len(data_chunks) == chunk_size:
                    break
            curr = "".join(data_chunks)

            if curr != prev:
                prev_lines = prev.split('\n')
                curr_lines = curr.split('\n')

                for line in curr_lines[len(curr_lines)-chunk_size:]:
                    if line not in prev_lines:
                        pipe.write(f"New Entry: {line}")

                prev = curr

async def read_from_pipe():
    with open(pipe_name, 'r') as pipe:
        while True:
            line = pipe.readline()
            if not line:
                await asyncio.sleep(0.1)
            print("Received:", line.strip())
