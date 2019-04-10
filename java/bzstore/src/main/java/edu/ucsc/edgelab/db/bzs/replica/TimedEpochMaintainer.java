package edu.ucsc.edgelab.db.bzs.replica;

import java.util.TimerTask;

public class TimedEpochMaintainer extends TimerTask {

    private TransactionProcessor transactionProcessor;

    public void setProcessor(TransactionProcessor tp) {
        this.transactionProcessor = tp;
    }

    @Override
    public void run() {
        transactionProcessor.resetEpoch(true);
    }
}
