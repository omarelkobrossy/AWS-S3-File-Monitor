#!/usr/bin/env python
import argparse
from utils import *
import pandas
import s3parser as s3p
import data_flow_test as dft
import threading

#RATE_LIMIT = 3500           --S3 Retrieval Rate Limit

# Create an argument parser
parser = argparse.ArgumentParser(description="Argument parsing example")

# Add mutually exclusive group for --connect and --local
local_or_URL = parser.add_mutually_exclusive_group(required=True)
getData_or_Monitor = parser.add_mutually_exclusive_group(required=False)
# root_traverse_group = parser.add_mutually_exclusive_group(required=True)

# Add --connect argument
local_or_URL.add_argument("-connect", type=validate_url, help="DB Link to connect to")

# Add --local argument
local_or_URL.add_argument("-local", type=str, help="DB Local file path")

# Create a mutually exclusive group for --root and --traverse
# root_traverse_group.add_argument("root", action="store_true", help="Use root mode")
# root_traverse_group.add_argument("traverse", action="store_true", help="Use traverse mode")

#Add --chunk argument
getData_or_Monitor.add_argument("-chunk", type=int, help="Add the chunk size")
getData_or_Monitor.add_argument("-monitor", action="store_true", help="Monitor the Flow of data")

# Parse the arguments
args = parser.parse_args()

# Access parsed arguments
connect_link = args.connect
local_path = args.local
chunk = args.chunk
monitor_flag = args.monitor

# If -connect was used
if connect_link:
    resource_type = detect_resource_type(connect_link)
    print(f"Detected Resource: {resource_type}")
    if not monitor_flag:                                    #If you connect but you don't want to monitor the data, just retrieve it with a chunk size (optional)
        data = s3p.getS3Data(url=connect_link, 
                             chunk_size=chunk)
        print(data)
    else:
        bucket_name, file_path = s3p.traverseS3Objects(connect_link)

        #Data Flow Testing function --- For testing purposes only
        #data_flow_thread = threading.Thread(
        #    target=dft.run_data_flow, 
        #    args=(s3p.s3, bucket_name, file_path))
        #Main Monitoring function
        monitor_s3_thread = threading.Thread(
            target=s3p.monitor_s3_file, 
            args=(s3p.s3, connect_link))

        # Start the threads
        #data_flow_thread.start()
        monitor_s3_thread.start()
        #data_flow_thread.join()
        monitor_s3_thread.join()
