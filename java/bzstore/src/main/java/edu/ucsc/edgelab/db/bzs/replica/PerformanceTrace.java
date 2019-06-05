package edu.ucsc.edgelab.db.bzs.replica;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PerformanceTrace {

    public static final Logger log = Logger.getLogger(PerformanceTrace.class.getName());
    private int metricCount;
    private Map<Integer, Metrics> batchMetrics = new TreeMap<>();
    private Map<TransactionID, TransactionMetrics> transactionMetrics = new TreeMap<>();
    private ReportBuilder reportBuilder;

    public PerformanceTrace() {
        String[] fields = new String[]{"Epoch Number, ",    // 1 Done
                "Total Txns in Epoch, ",                    // 2 Done
                "Total Local Txns in Epoch, ",              // 3 Done
                "Total Remote Txns in Epoch, ",             // 4 Done
                "Local Txns Prepared Count, ",              // 5 Done
                "Distributed Txns Prepared Count, ",        // 6 Done
                "Local Txns Committed In Epoch, ",          // 7 Done
                "Distributed Txns CommittedIn Epoch, ",     // 8 Done
                "Local Txns Failed In Epoch(F), ",          // 9 Done
                "Distributed Txns Failed In Epoch(F), ",    // 10 Done
                "Total Txns Committed, ",                   // 11 Sum (7, 8)
                "Total Txns Failed, ",                      // 12 Sum (9,10)
                "Local Txns Prepare Time(ms), ",            // 13
                "Distributed Txns Prepare Time(ms), ",      // 14
                "Local Txn Commit Time(ms), ",              // 15
                "Dist Txn Commit Time(ms), ",               // 16
                "Batch Completion Time(ms), ",              // 17 StopTime - StartTime
                "Local Txn Throughput(Tps), ",              // 18
                "Distributed Txn Throughput(Tps), ",        // 19
                "Local Txn Bytes processed (Bytes), ",      // 20
                "Remote Txn Bytes processed (Bytes), ",     // 21
                "Local Txn Throughput (Bps), ",             // 22
                "Remote Txn Throughput (Bps), ",            // 23
                "Throughput (Bps)\n"                        // 24
        };
        metricCount = 0;
        try {
            reportBuilder = new ReportBuilder("WedgeDB_perf_report", fields);
        } catch (IOException e) {
            log.log(Level.WARNING, "Exception occurred while creating the report builder. " + e.getLocalizedMessage(), e);
        }
    }

    public void writeToFile() {
        for (int batchnumber : batchMetrics.keySet()) {
            Metrics m = batchMetrics.get(batchnumber);
            reportBuilder.writeLine(String.format(
                    "%d, %d, %d, %d, %d, %d, %d, %d, %d, " +
                            "%d, %d, %d, %d, %d, %d, %d, %d, %d, " +
                            "%d, %d, %d, %d, %d, %d\n "
                    // TODO add more parameters for the performance analysis.

            ));
        }
    }

    public enum TimingMetric {
        localPrepareStartTime,
        localPrepareEndTime,
        remotePrepareStartTime,
        remotePrepareEndTime,
        localCommitTime,
        remoteCommitTime
    }

    public enum BatchMetric {
        prepareBatchNumber,
        commitBatchNumber
    }

    public void setTransactionTimingMetric(final TransactionID tid, TimingMetric metric, final long time) {
        TransactionMetrics m = getTimingMetric(tid);
        synchronized (m) {
            switch (metric) {
                case localPrepareStartTime:
                    m.prepareStartTime = time;
                    break;
                case localPrepareEndTime:
                    m.prepareEndTime = time;
                    break;
                case remotePrepareStartTime:
                    m.remotePrepareStartTime = time;
                    break;
                case remotePrepareEndTime:
                    m.remotePrepareEndTime = time;
                case localCommitTime:
                    m.localCommitTime = time;
                    break;
                case remoteCommitTime:
                    m.remoteCommitTime = time;
                    break;
            }
        }
    }

    public void setTidBatchInfo(TransactionID tid, BatchMetric metric, int value) {
        TransactionMetrics m = getTimingMetric(tid);
        synchronized (m) {
            switch (metric) {
                case prepareBatchNumber:
                    m.prepareBatch = value;
                    break;
                case commitBatchNumber:
                    m.commitBatch = value;
                    break;
            }
        }
    }

    private TransactionMetrics getTimingMetric(TransactionID tid) {
        if (!transactionMetrics.containsKey(tid)) {
            transactionMetrics.put(tid, new TransactionMetrics());
        }
        return transactionMetrics.get(tid);
    }

    private Metrics getMetricsForBatch(final Integer batchNumber) {
        if (!batchMetrics.containsKey(batchNumber)) {
            batchMetrics.put(batchNumber, new Metrics());
        }
        return batchMetrics.get(batchNumber);
    }

    public void setTotalTransactionCount(final Integer batchNumber,
                                         final Integer totalTransactionCount,
                                         final Integer localTransactionCount,
                                         final Integer remoteTransactionCount) {
        Metrics m = getMetricsForBatch(batchNumber);
        m.batchNumber = batchNumber;
        m.localTransactionCount = localTransactionCount;
        m.remoteTransactionCount = remoteTransactionCount;
        m.transactionCount = totalTransactionCount;
    }

    public void setBatchStartTime(final Integer batchNumber, final long batchStartTime) {
        Metrics m = getMetricsForBatch(batchNumber);
        m.batchStartTime = batchStartTime;
    }

    public void setBatchStopTime(final Integer batchNumber, final long batchEndTime) {
        Metrics m = getMetricsForBatch(batchNumber);
        m.batchEndTime = batchEndTime;
    }

    public void incrementLocalPreparedCount(final Integer batchNumber, final Integer count) {
        Metrics m = getMetricsForBatch(batchNumber);
        synchronized (m) {
            m.localPrepared += count;
        }
    }

    public void incrementDistributedPreparedCount(final Integer batchNumber, final Integer count) {
        Metrics m = getMetricsForBatch(batchNumber);
        synchronized (m) {
            m.distributedPrepared += count;
        }
    }

    public void incrementDistributedCompletedCount(final Integer batchNumber, final Integer count) {
        Metrics m = getMetricsForBatch(batchNumber);
        synchronized (m) {
            m.distributedCompleted += count;
        }
    }

    public void incrementLocalCompletedCount(final Integer batchNumber, final Integer count) {
        Metrics m = getMetricsForBatch(batchNumber);
        synchronized (m) {
            m.localCompleted += count;
        }
    }

    public void incrementLocalCommitFailedCount(final Integer batchNumber, final Integer count) {
        Metrics m = getMetricsForBatch(batchNumber);
        synchronized (m) {
            m.localTransactionsFailed += count;
        }
    }

    public void incrementDistributedCommitFailedCount(final Integer batchNumber, final Integer count) {
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

    long localPrepareStartTime = 0;
    long distributedPrepareStartTime = 0;

    long localPrepareEndTime = 0;
    long distributedPrepareEndTime = 0;


    int localTransactionsFailed = 0;
    int remoteTransactionsFailed = 0;

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

class TransactionMetrics {
    int prepareBatch;
    int commitBatch;
    long prepareStartTime;
    long prepareEndTime;
    long remotePrepareStartTime;
    long remotePrepareEndTime;
    long localCommitTime;
    long remoteCommitTime;
}