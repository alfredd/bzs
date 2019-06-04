package edu.ucsc.edgelab.db.bzs.replica;

import java.util.HashMap;
import java.util.Map;

public class PerformanceTrace {

    private Map<Integer, Metrics> batchMetrics = new HashMap<>();

    private Metrics getMetricsForBatch(Integer batchNumber) {
        if (!batchMetrics.containsKey(batchNumber)) {
            batchMetrics.put(batchNumber, new Metrics());
        }
        return batchMetrics.get(batchNumber);
    }

    public void setTotalTransactionCount(Integer batchNumber,
                                         Integer totalTransactionCount,
                                         Integer localTransactionCount,
                                         Integer remoteTransactionCount) {
        Metrics m = getMetricsForBatch(batchNumber);
        m.batchNumber = batchNumber;
        m.localTransactionCount = localTransactionCount;
        m.remoteTransactionCount = remoteTransactionCount;
        m.transactionCount = totalTransactionCount;
    }

    public void setBatchStartTime(Integer batchNumber) {
        Metrics m = getMetricsForBatch(batchNumber);
        m.batchStartTime = System.currentTimeMillis();
    }

    public void setBatchStopTime(Integer batchNumber) {
        Metrics m = getMetricsForBatch(batchNumber);
        m.batchEndTime = System.currentTimeMillis();
    }

    public void incrementLocalPreparedCount(Integer batchNumber, Integer count) {
        Metrics m = getMetricsForBatch(batchNumber);
        synchronized (m) {
            m.localPrepared += count;
        }
    }

    public void incrementDistributedPreparedCount(Integer batchNumber, Integer count) {
        Metrics m = getMetricsForBatch(batchNumber);
        synchronized (m) {
            m.distributedPrepared += count;
        }
    }

    public void incrementDistributedCompletedCount(Integer batchNumber, Integer count) {
        Metrics m = getMetricsForBatch(batchNumber);
        synchronized (m) {
            m.distributedCompleted += count;
        }
    }

    public void incrementLocalCompletedCount(Integer batchNumber, Integer count) {
        Metrics m = getMetricsForBatch(batchNumber);
        synchronized (m) {
            m.localCompleted += count;
        }
    }

    public void incrementLocalCommitFailedCount(Integer batchNumber, Integer count) {
        Metrics m = getMetricsForBatch(batchNumber);
        synchronized (m) {
            m.localTransactionsFailed += count;
        }
    }

    public void incrementDistributedCommitFailedCount(Integer batchNumber, Integer count) {
        Metrics m = getMetricsForBatch(batchNumber);
        synchronized (m) {
            m.remoteTransactionsFailed += count;
        }
    }


    public static void main(String[] args) {
        PerformanceTrace performanceTrace = new PerformanceTrace();
        performanceTrace.setTotalTransactionCount(1, 100, 10, 90);
        System.out.println(performanceTrace.batchMetrics.get(1));
    }
}


class Metrics {
    int transactionCount = 0;
    int localTransactionCount = 0;
    int remoteTransactionCount = 0;
    int batchNumber = 0;

    long batchStartTime = 0;
    long batchEndTime = 0;

    //    int localTransactionsCompleted = 0;
    int localTransactionsFailed = 0;
    int remoteTransactionsFailed = 0;

//    int remoteTransactionsCompleted = 0;

    int localPrepared = 0;
    int distributedPrepared = 0;


    int localCompleted = 0;
    int distributedCompleted = 0;

    @Override
    public String toString() {
        return String.format("transactionCount = %d \n" +
                        "localTransactionCount = %d\n" +
                        "remoteTransactionCount = %d\n" +
                        "batchNumber = %d \n" +
                        "batchStartTime = %d\n" +
                        "batchEndTime %d \n" +
//                        "localTransactionsCompleted = %d \n" +
                        "localTransactionsFailed = %d \n" +
//                        "remoteTransactionsCompleted = %d \n" +
                        "remoteTransactionsFailed = %d \n" +
                        "localPrepared = %d \n" +
                        "distributedPrepared = %d \n" +
                        "localCompleted = %d \n" +
                        "distributedCompleted = %d ",
                transactionCount,
                localTransactionCount,
                remoteTransactionCount,
                batchNumber,
                batchStartTime,
                batchEndTime,
//                localTransactionsCompleted,
                localTransactionsFailed,
//                remoteTransactionsCompleted,
                remoteTransactionsFailed,
                localPrepared,
                distributedPrepared,
                localCompleted,
                distributedCompleted
        );
    }
}