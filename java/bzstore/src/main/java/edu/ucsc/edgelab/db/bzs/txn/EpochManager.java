package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.replica.TransactionID;

public class EpochManager {
    private volatile Integer epochNumber = 0;
    private volatile Integer sequenceNumber = 0;
    private volatile long epochStartTime = 0;

    public TransactionID getTID() {
        final Integer epochNumber = this.epochNumber;
        final Integer sequenceNumber = this.sequenceNumber;
        this.sequenceNumber+=1;
        return new TransactionID(epochNumber, sequenceNumber);
    }

    public void setEpochStartTime(final long epochStartTime) {
        this.epochStartTime = epochStartTime;
    }

    public long getEpochStartTime() {
        return epochStartTime;
    }

    public void updateEpoch() {
        System.currentTimeMillis();
    }

    public void processEpoch() {
        final Integer sequence = sequenceNumber;
        sequenceNumber=0;
        final Integer epoch = epochNumber;
        epochNumber+=1;
        EpochProcessor epochProcessor = new EpochProcessor(epoch);
        epochProcessor.processEpoch(sequence);
    }
}
