package edu.ucsc.edgelab.db.bzs.replica;

public class Metrics {
    public int getTransactionCount() {
        return transactionCount;
    }

    public void updateTransactionCount(int transactionCount) {
        this.transactionCount = transactionCount;
    }

    public int getLocalTransactionCount() {
        return localTransactionCount;
    }

    public void updateLocalTransactionCount(int localTransactionCount) {
        this.localTransactionCount = localTransactionCount;
    }

    public int getRemoteTransactionCount() {
        return remoteTransactionCount;
    }

    public void updateRemoteTransactionCount(int remoteTransactionCount) {
        this.remoteTransactionCount = remoteTransactionCount;
    }

    public int getEpochNumber() {
        return epochNumber;
    }

    public void setEpochNumber(int batchNumber) {
        this.epochNumber = batchNumber;
    }

    public long getEpochStartTime() {
        return batchStartTime;
    }

    public void setEpochStartTime(long batchStartTime) {
        this.batchStartTime = batchStartTime;
    }

    public long getEpochEndTime() {
        return batchEndTime;
    }

    public void setEpochEndTime(long batchEndTime) {
        this.batchEndTime = batchEndTime;
    }

    public long getLRWTPrepareStartTime() {
        return localPrepareStartTime;
    }

    public void setLRWTPrepareStartTime(long localPrepareStartTime) {
        this.localPrepareStartTime = localPrepareStartTime;
    }

    public long getDRWTPrepareStartTime() {
        return distributedPrepareStartTime;
    }

    public void setDRWTPrepareStartTime(long distributedPrepareStartTime) {
        this.distributedPrepareStartTime = distributedPrepareStartTime;
    }

    public long getLRWTPrepareEndTime() {
        return localPrepareEndTime;
    }

    public void setLRWTPrepareEndTime(long localPrepareEndTime) {
        this.localPrepareEndTime = localPrepareEndTime;
    }

    public long getDRWTPrepareEndTime() {
        return distributedPrepareEndTime;
    }

    public void setDRWTPrepareEndTime(long distributedPrepareEndTime) {
        this.distributedPrepareEndTime = distributedPrepareEndTime;
    }

    public int getBytesPreparedInEpoch() {
        return bytesPreparedInEpoch;
    }

    public void setBytesPreparedInEpoch(int bytesPreparedInEpoch) {
        this.bytesPreparedInEpoch = bytesPreparedInEpoch;
    }

    public int getBytesCommittedInEpoch() {
        return bytesCommittedInEpoch;
    }

    public void setBytesCommittedInEpoch(int bytesCommittedInEpoch) {
        this.bytesCommittedInEpoch = bytesCommittedInEpoch;
    }

    public long getSMRCommitEndTime() {
        return localCommitEndTime;
    }

    public void setSMRCommitEndTime(long localCommitEndTime) {
        this.localCommitEndTime = localCommitEndTime;
    }

    @Deprecated
    public long getLocalCommitStartTime() {
        return localCommitStartTime;
    }

    @Deprecated
    public void setLocalCommitStartTime(long localCommitStartTime) {
        this.localCommitStartTime = localCommitStartTime;
    }

    public int getLocalTransactionsFailed() {
        return localTransactionsFailed;
    }

    public void setLocalTransactionsFailed(int localTransactionsFailed) {
        this.localTransactionsFailed = localTransactionsFailed;
    }

    public int getRemoteTransactionsFailed() {
        return remoteTransactionsFailed;
    }

    public void setRemoteTransactionsFailed(int remoteTransactionsFailed) {
        this.remoteTransactionsFailed = remoteTransactionsFailed;
    }

    public int getLocalPrepared() {
        return localPrepared;
    }

    public void setLocalPrepared(int localPrepared) {
        this.localPrepared = localPrepared;
    }

    public int getDistributedPrepared() {
        return distributedPrepared;
    }

    public void setDistributedPrepared(int distributedPrepared) {
        this.distributedPrepared = distributedPrepared;
    }

    public int getLocalCompleted() {
        return localCompleted;
    }

    public void setLocalCompleted(int localCompleted) {
        this.localCompleted = localCompleted;
    }

    public int getDistributedCompleted() {
        return distributedCompleted;
    }

    public void setDistributedCompleted(int distributedCompleted) {
        this.distributedCompleted = distributedCompleted;
    }

    int transactionCount = 0;
    int localTransactionCount = 0;
    int remoteTransactionCount = 0;
    int epochNumber = 0;

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
                        "epochNumber = %d \n" +
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
                epochNumber,
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
