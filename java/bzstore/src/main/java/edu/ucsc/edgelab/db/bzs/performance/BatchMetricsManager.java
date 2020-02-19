package edu.ucsc.edgelab.db.bzs.performance;

import edu.ucsc.edgelab.db.bzs.replica.TransactionID;

import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BatchMetricsManager {
    ConcurrentHashMap<Integer, BatchMetrics> batchMetrics = new ConcurrentHashMap<>();
    private static Logger log = Logger.getLogger(BatchMetricsManager.class.getName());

    public void setInitialBatchMetrics(int epoch, TransactionID tid) {
        if (!batchMetrics.containsKey(epoch)) {
            BatchMetrics metrics = new BatchMetrics();
            metrics.startTime = System.currentTimeMillis();
            metrics.txnStartedCount = 1;
            batchMetrics.put(epoch, metrics);
        }/* else {
            BatchMetrics metrics = batchMetrics.get(epoch);
            metrics.txnStartedCount += 1;
            metrics.endTime = System.currentTimeMillis();
            batchMetrics.put(epoch, metrics);
        }*/
    }

    public void setBatchMetrics(TransactionID tid) {
        Integer epochNumber = tid.getEpochNumber();
        if (!batchMetrics.containsKey(epochNumber)) {
            log.log(Level.WARNING, "Could not find entry for epoch "+epochNumber+" in batch metrics manager");
        } else {
            BatchMetrics metrics = batchMetrics.get(epochNumber);
            metrics.txnStartedCount += 1;
            metrics.endTime = System.currentTimeMillis();
            batchMetrics.put(epochNumber, metrics);
        }
    }

    public ConcurrentHashMap<Integer, BatchMetrics> getBatchMetrics() {
        return batchMetrics;
    }
}

