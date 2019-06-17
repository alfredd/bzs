package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;

public class EpochManager {
    private volatile Integer epochNumber = 0;
    private volatile Integer sequenceNumber = 0;
    private volatile long epochStartTime;
    private EpochThreadPoolExecutor epochThreadPoolExecutor;
    public static final int EPOCH_BUFFER = 5;


    public EpochManager() {
        epochStartTime = System.currentTimeMillis();
        epochThreadPoolExecutor = new EpochThreadPoolExecutor();
    }

    public TransactionID getTID() {
        updateEpoch();
        this.sequenceNumber += 1;
        final Integer epochNumber = this.epochNumber;
        final Integer sequenceNumber = this.sequenceNumber;
        return new TransactionID(epochNumber, sequenceNumber);
    }

    public void setEpochStartTime(final long epochStartTime) {
        this.epochStartTime = epochStartTime;
    }

    public long getEpochStartTime() {
        return epochStartTime;
    }

    private Integer updateEpoch() {
        final long currentTime = System.currentTimeMillis();
        long duration = getEpochStartTime() - currentTime;
        Integer seq = -1;
        if (duration > Configuration.MAX_EPOCH_DURATION_MS) {
            if (sequenceNumber > 0) {
                seq = sequenceNumber;
            }
        } else if (sequenceNumber > Configuration.MAX_EPOCH_TXN) {
            seq = sequenceNumber;
        }
        if (seq > 0) {
            final int epoch = epochNumber;
            setEpochStartTime(currentTime);
            epochNumber += 1;
            sequenceNumber = -1;
            processEpoch(epoch, seq+EPOCH_BUFFER);
        }
        return seq;
    }

    protected void processEpoch(final Integer epoch, final Integer txnCount) {
        epochThreadPoolExecutor.addToThreadPool(new EpochProcessor(epoch, txnCount));
    }
}
