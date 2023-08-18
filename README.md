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
    {
      "time": "22:43:00",
      "operation": "ADD",
      "added_row_number": 132,
      "added_row": "YSRILOHWLV,21,77397,P-21"
    },
    {
      "time": "22:43:01",
      "operation": "MODIFY",
      "modified_row_number": 128,
      "pre_modified": "IHXGH383IW,48,55810,Employed",
      "post_modification": "IFJ5APJT9K,16,70067,P-16"
    },
    {
      "time": "22:43:06",
      "operation": "DELETE",
      "deleted_row_number": 132,
      "deleted_row": "YSRILOHWLV,21,77397,P-21"
    },
    {
      "time": "22:43:07",
      "operation": "ADD",
      "added_row_number": 132,
      "added_row": "NU02RQWSL5,40,81179,P-40"
    },
    {
      "time": "22:43:07",
      "operation": "MODIFY",
      "modified_row_number": 94,
      "pre_modified": "SWLIA781DT,35,48083,Employed",
      "post_modification": "6U8ZM0JMXM,25,72005,P-25"
    },
    {
      "time": "22:43:12",
      "operation": "DELETE",
      "deleted_row_number": 132,
      "deleted_row": "NU02RQWSL5,40,81179,P-40"
    },
    {
      "time": "22:43:13",
      "operation": "ADD",
      "added_row_number": 132,
      "added_row": "C45A1AYV66,16,84892,P-16"
    },
    {
      "time": "22:43:13",
      "operation": "MODIFY",
      "modified_row_number": 114,
      "pre_modified": "Y5JB70SNCR,38,83193,Employed",
      "post_modification": "C1B4A1LQV9,27,56934,P-27"
    },
    {
      "time": "22:45:13",
      "operation": "DELETE",
      "deleted_row_number": 132,
      "deleted_row": "C45A1AYV66,16,84892,P-16"
    },
    {
      "time": "22:45:13",
      "operation": "ADD",
      "added_row_number": 132,
      "added_row": "7SI7F1BAL1,31,51596,P-31"
    },
    {
      "time": "22:45:14",
      "operation": "MODIFY",
      "modified_row_number": 53,
      "pre_modified": "4UVGQ6Z5IK,34,85959,Employed",
      "post_modification": "8QRSZ2MEK3,34,92604,P-34"
    },
    {
      "time": "22:45:19",
      "operation": "DELETE",
      "deleted_row_number": 132,
      "deleted_row": "7SI7F1BAL1,31,51596,P-31"
    },
    {
      "time": "22:45:19",
      "operation": "ADD",
      "added_row_number": 132,
      "added_row": "WVEP87CQRF,41,69617,P-41"
    },
    {
      "time": "22:45:20",
      "operation": "MODIFY",
      "modified_row_number": 20,
      "pre_modified": "CIEDU7IHPI,33,34908,Employed",
      "post_modification": "UF681RVSJ2,41,97604,P-41"
    },
    {
      "time": "22:45:28",
      "operation": "DELETE",
      "deleted_row_number": 132,
      "deleted_row": "WVEP87CQRF,41,69617,P-41"
    },
    {
      "time": "22:45:28",
      "operation": "ADD",
      "added_row_number": 132,
      "added_row": "3F2IEK7IPX,43,75756,P-43"
    },
    {
      "time": "22:45:28",
      "operation": "MODIFY",
      "modified_row_number": 64,
      "pre_modified": "DTXP2ZA3ZI,38,38962,Employed",
      "post_modification": "VMBTYCMQYR,18,47028,P-18"
    }
  ]
