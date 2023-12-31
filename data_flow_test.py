import random
import string
import time


def append_to_csv(s3, bucket, file_key, data):
    response = s3.get_object(Bucket=bucket, Key=file_key)
    existing_csv = response['Body'].read().decode('utf-8')
    updated_csv = existing_csv + '\n' + data
    s3.put_object(Bucket=bucket, Key=file_key,
                  Body=updated_csv.encode('utf-8'))

def delete_from_csv(s3, bucket, file_key):
    response = s3.get_object(Bucket=bucket, Key=file_key)
    data = response['Body'].read().decode('utf-8')
    rows = len(data.split('\n'))
    updated_data = "\n".join(data.split('\n')[0:rows-1])
    s3.put_object(Bucket=bucket, 
                  Key=file_key,
                  Body=updated_data.encode('utf-8'))
    

def modify_from_csv(s3, bucket, file_key):
    response = s3.get_object(Bucket=bucket, Key=file_key)
    data = response['Body'].read().decode('utf-8')
    data_list = data.split('\n')
    rows = len(data_list)
    random_row_modify = random.randint(0, rows-1)
    data_list[random_row_modify] = generate_random_data()
    updated_data = "\n".join(data_list)
    s3.put_object(Bucket=bucket,
                  Key=file_key,
                  Body=updated_data.encode('utf-8'))


def generate_random_data():
    param1 = ''.join(random.choice(
        string.ascii_uppercase + string.digits) for _ in range(10))
    param2 = random.randint(15, 50)
    param3 = random.randint(30000, 100000)
    return f'{param1},{param2},{param3},P-{param2}'



def run_data_flow(s3, bucket_name, file_key):
    try:
        while True:
            random_data = generate_random_data()
            delete_from_csv(s3, bucket_name, file_key)
            time.sleep(5)
            append_to_csv(s3, bucket_name, file_key, random_data)
            print(f"Test Data Added: {random_data}")
            time.sleep(5)
            modify_from_csv(s3, bucket_name, file_key)
            time.sleep(5)
    except KeyboardInterrupt:
        print("Data generation stopped.")

