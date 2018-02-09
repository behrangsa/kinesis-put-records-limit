# Kinesis Data Streams Put Records Limit

According to the docs:

But, looks like the 1000 records per second per shard limit is not strict and it is possible to exceed this limit from
time to time.

For example, with 1 shard, it was possible to put ~5000 records per second into a Kinesis stream:

```text
22:36:20.793 [        main] INFO  com.github.behrangsa.KinesisPutRecordsLimitTest - Number of records successfully put: 5000
22:36:20.793 [        main] INFO  com.github.behrangsa.KinesisPutRecordsLimitTest - Number of failures: 0
22:36:20.793 [        main] INFO  com.github.behrangsa.KinesisPutRecordsLimitTest - Test duration: PT0.997S
22:36:20.793 [        main] INFO  com.github.behrangsa.KinesisPutRecordsLimitTest - Success per second: 5015.045135406219
```