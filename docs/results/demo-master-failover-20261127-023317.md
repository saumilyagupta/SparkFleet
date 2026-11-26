# Master failover (20261127-023317)

- Killed: active master system-02; new master: system-01
- **Result identical: True**

| time | event |
|---|---|
| +   1.3s | masters: system-02=ALIVE (will be killed), system-01=STANDBY |
| +   3.2s | job started with master URL spark://172.22.68.221:7077,172.22.68.216:7077 (pid 49989, 864 partitions) |
| +  18.9s | stopped spark-master on system-02: the active master is DOWN |
| +  52.2s | ZooKeeper elected system-01: standby master ALIVE after 33s; the job is still running |
| + 144.4s | the running job finished under the new master |
| + 156.8s | new application submitted after failover ran on system-01: result identical=True |
| + 161.8s | system-02 restarted and rejoined as STANDBY |
| + 161.8s | results identical to single-machine count: True |

```
26/11/27 02:33:19 INFO SparkContext: Submitted application: WordCount-master-failover-20261127-023317
26/11/27 02:33:20 INFO StandaloneAppClient$ClientEndpoint: Connecting to master spark://172.22.68.216:7077...
26/11/27 02:33:20 INFO StandaloneAppClient$ClientEndpoint: Connecting to master spark://172.22.68.221:7077...
26/11/27 02:33:34 WARN StandaloneAppClient$ClientEndpoint: Connection to 172.22.68.216:7077 failed; waiting for master to reconnect...
26/11/27 02:33:34 WARN StandaloneAppClient$ClientEndpoint: Connection to 172.22.68.216:7077 failed; waiting for master to reconnect...
26/11/27 02:34:04 INFO StandaloneAppClient$ClientEndpoint: Master has changed, new master is at spark://172.22.68.221:7077
output=/opt/spark-data/output/master-failover-20261127-023317.tsv
26/11/27 02:35:28 INFO DAGScheduler: ShuffleMapStage 0 (reduceByKey at /opt/spark-data/jobs/wordcount.py:66) finished in 127.569 s
26/11/27 02:35:31 INFO DAGScheduler: Job 0 finished: sortBy at /opt/spark-data/jobs/wordcount.py:71, took 130.326803 s
WORDCOUNT_DONE app_id=app-20261127023320-0000 driver=cselab2pc36
total_words=25327776 distinct_words=99 elapsed_s=136.5
```
