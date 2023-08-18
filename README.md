# AWS-S3-File-Monitor

## Usage
```python main.py -connect s3_link```
Retrieves the whole data from that file

```python main.py -connect s3_link -chunk 5```
Retrieves the first 5 rows of the data from that file

```python main.py -connect s3_link -monitor```
Begins monitoring every change made on that file (Insertion, Deletion, Modification)

You can add ```-flowtest``` along with ```-monitor``` to feed some random data if you want to test it, and you can modify the test data fed by editing the content of the ``data_flow_test.py`` file

Here is an example of how the program would work:
```
  D:\code\projects\DBA>python main.py -connect https://my-aws-bucket-v1.s3.eu-north-1.amazonaws.com/2023/07/17/testCSV -monitor -flowtest -log
  Detected Resource: S3 Bucket
  File Paths Display (Choose one of those by number): >>>
  =======================
  0: 2023/07/17/testCSV/
  1: 2023/07/17/testCSV/test_data.csv
  2: 2023/07/17/testCSV/test_text.txt
  Path Number: 1
  Monitoring on path: my-aws-bucket-v1, 2023/07/17/testCSV/test_data.csv
  Deleted Row 132: DDWKT3KP2S,25,63753,P-25
  Test Data Added: A2QDEXNL6T,22,72607,P-22
  New Row 132: A2QDEXNL6T,22,72607,P-22
  Modified Entry Row - 64: VMBTYCMQYR,18,47028,P-18 ===> 0RAJBUWOWX,35,67268,P-35
  Deleted Row 132: A2QDEXNL6T,22,72607,P-22
  Test Data Added: BODJG3FOVX,32,78251,P-32
  New Row 132: BODJG3FOVX,32,78251,P-32
  Modified Entry Row - 42: CDJ1DC98JO,34,39386,Employed ===> SWDDTP4HMG,22,56973,P-22
```

Adding ```-log``` with ```-monitor``` will log any changes made based on the date and hour, for example here's an example of the ```log.json``` file:
```
  "2023-08-17": [
    {
      "time": "22:42:54",
      "operation": "ADD",
      "added_row_number": 132,
      "added_row": "75JJGXC7WT,32,92865,P-32"
    },
    {
      "time": "22:42:55",
      "operation": "MODIFY",
      "modified_row_number": 35,
      "pre_modified": "BP5QXD90SF,23,55633,Employed",
      "post_modification": "BTWT4H14JU,47,41991,P-47"
    },
    {
      "time": "22:43:00",
      "operation": "DELETE",
      "deleted_row_number": 132,
      "deleted_row": "75JJGXC7WT,32,92865,P-32"
    },
  ]
```

Adding ```-delay``` followed up by a number `n` will check for changes every 'n' seconds, so the command would be:

```python main.py -connect s3_link -monitor -log -delay 5``` Which would make a request for change checking every 5 seconds.

