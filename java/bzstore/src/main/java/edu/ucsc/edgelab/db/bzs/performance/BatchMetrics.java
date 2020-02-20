package edu.ucsc.edgelab.db.bzs.performance;

public class BatchMetrics {
    public long startTime=0;
    public long epochCommitTime = 0;
    public long txnProcessingTime = 0;
    public int txnCommittedCount = 0;
    public int txnCompletedCount = 0;
}
