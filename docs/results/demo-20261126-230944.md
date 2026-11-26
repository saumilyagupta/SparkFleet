# Failure-recovery demo 20261126-230944

- Failure injected: **worker** on **system-02 (172.22.68.216)**
- Partitions: 288, artificial delay per partition: 12000 ms
- Baseline result: `baseline-20261126-230944.tsv`  sha256 `12af4f834e0941eb`
- Result with failure: `failure-20261126-230944.tsv`  sha256 `12af4f834e0941eb`
- **Identical: True**
- Full driver log: `demo-20261126-230944.log`

## Timeline

| time | event |
|------|-------|
| +   0.6s | baseline: clean run on 3 workers / 72 cores |
| +  13.5s | job started (pid 23697, 288 partitions, 12000 ms/partition) |
| +  16.4s | tasks running on system-02; injecting failure |
| +  17.4s | stopped spark-worker (worker JVM + its executors) on system-02 (172.22.68.216) |
| +  20.8s | master reports system-02 no longer ALIVE; job continues on survivors |
| + 105.1s | job finished despite the failure |
| + 105.2s | result identical to baseline: True |
| + 105.5s | restarted spark-worker on system-02 |
| + 109.8s | system-02 re-registered with the master |

## Driver log lines showing detection and recovery

```
26/11/26 23:09:58 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20261126230958-0009/0 on worker-20261126175706-172.22.68.218-46853 (172.22.68.218:46853) with 24 core(s)
26/11/26 23:09:58 INFO StandaloneSchedulerBackend: Granted executor ID app-20261126230958-0009/0 on hostPort 172.22.68.218:46853 with 24 core(s), 1024.0 MiB RAM
26/11/26 23:09:58 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20261126230958-0009/1 on worker-20261126175707-172.22.68.217-36887 (172.22.68.217:36887) with 24 core(s)
26/11/26 23:09:58 INFO StandaloneSchedulerBackend: Granted executor ID app-20261126230958-0009/1 on hostPort 172.22.68.217:36887 with 24 core(s), 1024.0 MiB RAM
26/11/26 23:09:58 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20261126230958-0009/2 on worker-20261126224708-172.22.68.216-43975 (172.22.68.216:43975) with 24 core(s)
26/11/26 23:09:58 INFO StandaloneSchedulerBackend: Granted executor ID app-20261126230958-0009/2 on hostPort 172.22.68.216:43975 with 24 core(s), 1024.0 MiB RAM
26/11/26 23:09:58 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20261126230958-0009/0 is now RUNNING
26/11/26 23:09:58 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20261126230958-0009/2 is now RUNNING
26/11/26 23:09:58 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20261126230958-0009/1 is now RUNNING
26/11/26 23:10:00 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20261126230958-0009/2 is now EXITED (Command exited with code 143)
26/11/26 23:10:00 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20261126230958-0009/3 on worker-20261126224708-172.22.68.216-43975 (172.22.68.216:43975) with 24 core(s)
26/11/26 23:10:00 INFO StandaloneSchedulerBackend: Granted executor ID app-20261126230958-0009/3 on hostPort 172.22.68.216:43975 with 24 core(s), 1024.0 MiB RAM
26/11/26 23:10:00 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20261126230958-0009/3 is now FAILED (java.lang.IllegalStateException: Shutdown hooks cannot be modified during shutdown.)
26/11/26 23:10:00 ERROR TaskSchedulerImpl: Lost executor 2 on 172.22.68.216: Command exited with code 143
26/11/26 23:10:00 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20261126230958-0009/4 on worker-20261126224708-172.22.68.216-43975 (172.22.68.216:43975) with 24 core(s)
26/11/26 23:10:00 INFO StandaloneSchedulerBackend: Granted executor ID app-20261126230958-0009/4 on hostPort 172.22.68.216:43975 with 24 core(s), 1024.0 MiB RAM
26/11/26 23:10:00 WARN TaskSetManager: Lost task 23.0 in stage 0.0 (TID 23) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 8.0 in stage 0.0 (TID 8) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 17.0 in stage 0.0 (TID 17) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 11.0 in stage 0.0 (TID 11) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 2.0 in stage 0.0 (TID 2) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 20.0 in stage 0.0 (TID 20) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 5.0 in stage 0.0 (TID 5) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 14.0 in stage 0.0 (TID 14) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 4.0 in stage 0.0 (TID 4) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 13.0 in stage 0.0 (TID 13) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 22.0 in stage 0.0 (TID 22) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 7.0 in stage 0.0 (TID 7) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 16.0 in stage 0.0 (TID 16) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 10.0 in stage 0.0 (TID 10) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 1.0 in stage 0.0 (TID 1) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 19.0 in stage 0.0 (TID 19) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 9.0 in stage 0.0 (TID 9) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 18.0 in stage 0.0 (TID 18) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 12.0 in stage 0.0 (TID 12) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 3.0 in stage 0.0 (TID 3) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 21.0 in stage 0.0 (TID 21) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 15.0 in stage 0.0 (TID 15) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 6.0 in stage 0.0 (TID 6) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 WARN TaskSetManager: Lost task 0.0 in stage 0.0 (TID 0) (172.22.68.216 executor 2): ExecutorLostFailure (executor 2 exited caused by one of the running tasks) Reason: Command exited with code 143
26/11/26 23:10:00 INFO DAGScheduler: Executor lost: 2 (epoch 0)
26/11/26 23:10:00 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20261126230958-0009/4 is now LOST (worker lost: 172.22.68.216:43975 got disassociated)
```
