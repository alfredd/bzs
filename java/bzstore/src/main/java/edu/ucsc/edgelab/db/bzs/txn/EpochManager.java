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

    public Integer updateEpoch() {
        final long currentTime = System.currentTimeMillis();
        long duration = getEpochStartTime() - currentTime;
        Integer seq = -1;
        if (duration > 30*1000) {
            if (sequenceNumber> 0) {
                seq = sequenceNumber;
            }
        } else if (sequenceNumber>2000){
            seq = sequenceNumber;
        }
        if (seq>0) {
            setEpochStartTime(currentTime);
            epochNumber+=1;
            sequenceNumber =0;
        }
        return seq;
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
