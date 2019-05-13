package edu.ucsc.edgelab.db.bzs.replica;

import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TimedEpochMaintainer extends TimerTask {

    private TransactionProcessor transactionProcessor;
    private static final Logger LOGGER = Logger.getLogger(TimedEpochMaintainer.class.getName());

    public void setProcessor(TransactionProcessor tp) {
        this.transactionProcessor = tp;
    }

    @Override
    public void run() {
        try {
            transactionProcessor.resetEpoch(true);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Exception occurred when processing transactions in epoch. "+ e.getLocalizedMessage(), e);
        }
    }
}
