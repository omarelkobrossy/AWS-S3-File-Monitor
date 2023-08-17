#!/usr/bin/env python
import argparse
from utils import *
import s3parser as s3p
import data_flow_test as dft
import threading

# Create an argument parser
parser = argparse.ArgumentParser(description="Argument parsing example")
getData_or_Monitor = parser.add_mutually_exclusive_group(required=False)
monitor_group = parser.add_mutually_exclusive_group()
# root_traverse_group = parser.add_mutually_exclusive_group(required=True)

# Add --connect argument
parser.add_argument("-connect", type=validate_url, help="DB Link to connect to")

#Add --chunk argument
getData_or_Monitor.add_argument("-chunk", type=int, help="Add the chunk size")
getData_or_Monitor.add_argument("-monitor", action="store_true", help="Monitor the Flow of data")
parser.add_argument("-flowtest", action="store_true", help="Test Flow of Data into a Test CSV")     #Argument to see whether you want to see tests

monitor_group.add_argument("-log", action="store_true", help="Enable logging while monitoring")

# Parse the arguments
args = parser.parse_args()

# Access parsed arguments
connect_link = args.connect
chunk = args.chunk
monitor_flag = args.monitor
flowtest = args.flowtest
log_flag = args.log

# If -connect was used
if connect_link:
    connect_link = validate_url(connect_link)
    resource_type = detect_resource_type(connect_link)
    print(f"Detected Resource: {resource_type}")
    bucket_name, file_path = s3p.traverseS3Objects(connect_link)
    s3p.create_iam_role_and_attach_to_bucket(bucket_name)
    if not monitor_flag:                                        #If you connect but you don't want to monitor the data, just retrieve it with a chunk size (optional)
        data = s3p.getS3Data(chunk_size=chunk,
                             bucket_name=bucket_name,
                             file_path=file_path)
        print(data)
    else:
        #Main Monitoring function
        monitor_s3_thread = threading.Thread(
            target=s3p.monitor_s3_file, 
            args=(s3p.s3,
                bucket_name,
                file_path,
                log_flag))
        monitor_s3_thread.start()
        if flowtest: #Data Flow Testing function --- For testing purposes only
            data_flow_thread = threading.Thread(
                target=dft.run_data_flow, 
                args=(s3p.s3, bucket_name, file_path))
            # Start the threads
            data_flow_thread.start()
            data_flow_thread.join()
        monitor_s3_thread.join()