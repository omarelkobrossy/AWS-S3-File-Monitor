import random
import string
import time


def append_to_csv(s3, bucket, file_key, data):
    response = s3.get_object(Bucket=bucket, Key=file_key)
    existing_csv = response['Body'].read().decode('utf-8')

    updated_csv = existing_csv + '\n' + data

    s3.put_object(Bucket=bucket, Key=file_key,
                  Body=updated_csv.encode('utf-8'))


def generate_random_data():
    name = ''.join(random.choice(
        string.ascii_uppercase + string.digits) for _ in range(10))
    age = random.randint(15, 50)
    salary = random.randint(30000, 100000)
    return f'{name},{age},{salary},Employed'



def run_data_flow(s3, bucket_name, file_key):
    try:
        while True:
            random_data = generate_random_data()
            append_to_csv(s3, bucket_name, file_key, random_data)
            print(f"Added: {random_data}")
            time.sleep(5)
    except KeyboardInterrupt:
        print("Data generation stopped.")

