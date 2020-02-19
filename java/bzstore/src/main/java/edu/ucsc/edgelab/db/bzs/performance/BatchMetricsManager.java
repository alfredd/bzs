package edu.ucsc.edgelab.db.bzs.performance;

import edu.ucsc.edgelab.db.bzs.replica.TransactionID;

import java.util.concurrent.ConcurrentHashMap;

public class BatchMetricsManager {
    ConcurrentHashMap<Integer, BatchMetrics> batchMetrics= new ConcurrentHashMap<>();


    public void setBatchMetrics(TransactionID tid) {
        Integer epochNumber = tid.getEpochNumber();
        if (!batchMetrics.contains(epochNumber)) {
            BatchMetrics value = new BatchMetrics();
            value.startTime=System.currentTimeMillis();
            value.txnCount=1;
            batchMetrics.put(epochNumber, value);
        } else {
            BatchMetrics metrics = batchMetrics.get(epochNumber);
            metrics.txnCount+=1;
            metrics.endTime=System.currentTimeMillis();
            batchMetrics.put(epochNumber, metrics);
        }
    }
    public ConcurrentHashMap<Integer, BatchMetrics> getBatchMetrics() {
        return batchMetrics;
    }
}

