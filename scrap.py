import csv
import boto3
s3 = boto3.client('s3')

chunk_size = 4
bucket_name = "my-aws-bucket-v1"
key = "2023/07/17/testCSV/test_data.csv"
response = s3.get_object(Bucket=bucket_name, Key=key)
data = response['Body']
file_size = int(response['ContentLength'])

def read_chunk(file, position):
    chunk_lines = []
    file.seek(position)
    while True:
        if position <= 0:
            break
        chunk = file.readline().decode('utf-8')
        if not chunk:
            break
        chunk_lines.append(chunk)
        position -= len(chunk)
        file.seek(position)
    return chunk_lines


prev_position = file_size
prev_lines = read_chunk(data, prev_position)

print(prev_lines)