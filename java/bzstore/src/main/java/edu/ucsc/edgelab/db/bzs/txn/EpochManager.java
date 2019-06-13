package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.replica.TransactionID;

public class EpochManager {
    private volatile Integer epochNumber = 0;
    private volatile Integer sequenceNumber = 0;

    public TransactionID getTID() {
        final Integer epochNumber = this.epochNumber;
        final Integer sequenceNumber = this.sequenceNumber;
        return new TransactionID(epochNumber, sequenceNumber);
    }
}
