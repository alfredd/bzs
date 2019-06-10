package edu.ucsc.edgelab.db.bzs.replica;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;

public class PerformanceTrace {

    public static final Logger log = Logger.getLogger(PerformanceTrace.class.getName());
    private int metricCount;
    private Map<Integer, Metrics> batchMetrics = new TreeMap<>();
    private Map<Integer, Set<TransactionID>> tidMap = new TreeMap<>();
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
                "Local Txns Prepare Time(ms), ",            // 13 Done
                "Distributed Txns Prepare Time(ms), ",      // 14 Done
                "Local Txn Commit Time(ms), ",              // 15 Done
                "Dist Txn Commit Time(ms), ",               // 16 NA
                "Batch Completion Time(ms), ",              // 17 StopTime - StartTime
                "Txn Throughput(Tps), ",                    // 18 Done
                "Prepared Txn throughput (Tps), ",          // 19 Done
                "Prepared Txn throughput (Bps), ",          // 20 Done
                "Throughput (Bps), ",                       // 21 Done
                "Distributed Txn Avg Prepare Time(s), ",    // 22 Done
                "Distributed Txn Avg Commit Time(s), ",     // 23 Done
                "Distributed Txn Avg Completion Time(s), ", // 24 Done
                "Distributed Txn Avg Epoch Span, \n ",      // 25 Done
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

            long latency = m.batchEndTime - m.batchStartTime;
            double throughputTps = latency == 0 ? 0 : (double) (m.localCompleted + m.distributedCompleted) * 1000 / (latency);
            double throughputBps = latency == 0 ? 0 : (double) m.bytesCommittedInEpoch * 1000 / (latency);
            double preparedTps = latency == 0 ? 0 : (double) (m.localPrepared + m.distributedPrepared) * 1000 / (latency);
            double preparedBps = latency == 0 ? 0 : (double) (m.bytesPreparedInEpoch) * 1000 / (latency);
            List<Long> completionTimes = new LinkedList<>();
            List<Long> prepareTimes = new LinkedList<>();
            List<Long> commitTimes = new LinkedList<>();
            List<Integer> batchSpans = new LinkedList<>();
            if (!tidMap.containsKey(batchnumber)) {
                Set<TransactionID> tids = tidMap.remove(batchnumber);
                if (tids != null) {
                    for (TransactionID tid : tids) {
                        TransactionMetrics tm = transactionMetrics.remove(tid);
                        if (tm.remoteCommitTime != 0 && tm.prepareStartTime != 0)
                            completionTimes.add(tm.remoteCommitTime - tm.prepareStartTime);
                        if (tm.remoteCommitTime != 0 && tm.localCommitTime != 0)
                            commitTimes.add(tm.remoteCommitTime - tm.localCommitTime);
                        if (tm.remotePrepareEndTime != 0 && tm.remotePrepareStartTime != 0)
                            prepareTimes.add(tm.remotePrepareEndTime - tm.remotePrepareStartTime);
                        if (tm.prepareBatch != 0 && (tm.commitBatch != 0 || tm.failedBatch != 0)) {
                            int completedBatch = tm.failedBatch;
                            if (tm.commitBatch != 0) {
                                completedBatch = tm.commitBatch;
                            }
                            batchSpans.add(tm.commitBatch - (completedBatch));
                        }
                    }
                }
            }

            double averageCompletionTime = 0.0;
            double averageCommitTime = 0.0;
            double averagePrepareTime = 0.0;
            double averageBatchSpan = 0.0;


            if (completionTimes.size() > 0)
                averageCompletionTime = (double) completionTimes.stream().mapToLong(i -> i.longValue()).sum() / completionTimes.size();
            if (batchSpans.size() > 0)
                averageBatchSpan = ((double) batchSpans.stream().mapToLong(i -> i.intValue()).sum()) / batchSpans.size();
            if (prepareTimes.size() > 0)
                averagePrepareTime = ((double) prepareTimes.stream().mapToLong(i -> i.longValue()).sum()) / prepareTimes.size();
            if (commitTimes.size() > 0)
                averageCommitTime = (double) commitTimes.stream().mapToLong(i -> i.longValue()).sum() / commitTimes.size();

            reportBuilder.writeLine(String.format(
                    "%d, %d, %d, %d, %d, %d, %d, %d, %d, " +
                            "%d, %d, %d, %d, %d, %d, %d, %d, %f, " +
                            "%f, %f, %f, %f, %f, %f, %f\n ",
                    // TODO add more parameters for the performance analysis.
                    m.batchNumber,
                    m.transactionCount, m.localTransactionCount, m.remoteTransactionCount, m.localPrepared, m.distributedPrepared,
                    m.localCompleted, m.distributedCompleted, m.localTransactionsFailed, m.remoteTransactionsFailed,
                    (m.localCompleted + m.distributedCompleted),
                    (m.localTransactionsFailed + m.remoteTransactionsFailed), (m.localPrepareEndTime - m.localPrepareStartTime),
                    (m.distributedPrepareEndTime - m.distributedPrepareStartTime),
                    (m.localCommitEndTime - m.localCommitStartTime), 0, (m.batchEndTime - m.batchStartTime), throughputTps, preparedTps,
                    preparedBps, throughputBps, averagePrepareTime, averageCommitTime, averageCompletionTime, averageBatchSpan

            ));

            batchMetrics.remove(batchnumber);
        }
    }

    public static void main(String[] args) {
        List<Integer> nums = new LinkedList<>();
        nums.add(1);
        nums.add(3);
        nums.add(2);
        nums.add(4);
        nums.add(5);
        nums.add(6);
        double avg = (double) nums.stream().mapToInt(i -> i.intValue()).sum() / nums.size();
        System.out.println(avg);

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
        commitBatchNumber,
        failedBatchNumber
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

    public void setTidBatchInfo(final TransactionID tid, BatchMetric metric, final int value) {
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
        if (!tidMap.containsKey(tid.getEpochNumber())) {
            tidMap.put(tid.getEpochNumber(), new HashSet<>());
        }
        tidMap.get(tid.getEpochNumber()).add(tid);
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
        synchronized (m) {
            m.batchStartTime = batchStartTime;
        }
    }

    public void setBatchStopTime(final Integer batchNumber, final long batchEndTime) {
        Metrics m = getMetricsForBatch(batchNumber);
        synchronized (m) {
            m.batchEndTime = batchEndTime;
        }
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

    public void incrementBytesPreparedInEpoch(final Integer batchNumber, final Integer count) {
        Metrics m = getMetricsForBatch(batchNumber);
        synchronized (m) {
            m.bytesPreparedInEpoch += count;
        }
    }


    public void incrementBytesCommittedInEpoch(final Integer batchNumber, final Integer count) {
        Metrics m = getMetricsForBatch(batchNumber);
        synchronized (m) {
            m.bytesCommittedInEpoch += count;
        }
    }


    public void setLocalPrepareEndTime(final Integer batchNumber, final long time) {
        Metrics m = getMetricsForBatch(batchNumber);
        synchronized (m) {
            m.localPrepareEndTime = time;
        }
    }

    public void setLocalPrepareStartTime(final Integer batchNumber, final long time) {
        Metrics m = getMetricsForBatch(batchNumber);
        synchronized (m) {
            m.localPrepareStartTime = time;
        }
    }

    public void setLocalCommitEndTime(final Integer batchNumber, final long time) {
        Metrics m = getMetricsForBatch(batchNumber);
        synchronized (m) {
            m.localCommitEndTime = time;
        }
    }

    public void setLocalCommitStartTime(final Integer batchNumber, final long time) {
        Metrics m = getMetricsForBatch(batchNumber);
        synchronized (m) {
            m.localCommitStartTime = time;
        }
    }

    public void setDistributedPrepareEndTime(final Integer batchNumber, final long time) {
        Metrics m = getMetricsForBatch(batchNumber);
        synchronized (m) {
            m.distributedPrepareEndTime = time;
        }
    }

    public void setDistributedPrepareStartTime(final Integer batchNumber, final long time) {
        Metrics m = getMetricsForBatch(batchNumber);
        synchronized (m) {
            m.distributedPrepareStartTime = time;
        }
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

    int bytesPreparedInEpoch = 0;
    int bytesCommittedInEpoch = 0;

    long localCommitEndTime = 0;
    long localCommitStartTime = 0;

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
    int prepareBatch = 0;
    int commitBatch = 0;
    int failedBatch = 0;
    long prepareStartTime = 0;
    long prepareEndTime = 0;
    long remotePrepareStartTime = 0;
    long remotePrepareEndTime = 0;
    long localCommitTime = 0;
    long remoteCommitTime = 0;
}