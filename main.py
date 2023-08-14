import argparse
from utils import *
import pandas
import s3parser as s3p
import subprocess
import asyncio

SUPPORTED = ['S3', 'SQL', '.csv', '.txt']
OPERATIONS = ['monitor', '']
#RATE_LIMIT = 3500           --S3 Retrieval Rate Limit

# Create an argument parser
parser = argparse.ArgumentParser(description="Argument parsing example")

# Add mutually exclusive group for --connect and --local
local_or_URL = parser.add_mutually_exclusive_group(required=True)
getData_or_Monitor = parser.add_mutually_exclusive_group(required=False)

# Add --connect argument
local_or_URL.add_argument("--connect", type=validate_url, help="DB Link to connect to")
# Add --local argument
local_or_URL.add_argument("--local", type=str, help="DB Local file path")
#Add --chunk argument
getData_or_Monitor.add_argument("--chunk", type=int, help="Add the chunk size")
getData_or_Monitor.add_argument("--monitor", type=int, help="Monitor the Flow of data")

# Parse the arguments
args = parser.parse_args()

# Access parsed arguments
connect_link = args.connect
local_path = args.local
chunk = args.chunk
monitor_flag = args.monitor
async def monitor():
    asyncio.create_task(s3p.read_from_pipe())
    await asyncio.sleep(0.5)  # Give a little time for read_from_pipe to start
    await s3p.monitorS3(connect_link, chunk)

# If --connect was used
if connect_link:
    resource_type = detect_resource_type(connect_link)
    # if "S3" in resource_type:
    #     data = s3p.getS3Data(connect_link, chunk)
    #     print(data)
    #     print("Data Loaded..")
if monitor_flag:
    #subprocess.Popen(['cmd.exe', '/k', 'type \\\\.\\pipe\\my_pipe'], creationflags=subprocess.CREATE_NEW_CONSOLE)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(monitor())
