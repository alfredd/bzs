package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;

import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DistributedTxnPreparedProcessorTimerTask extends TimerTask {
    private final TransactionProcessor processor;
    public static final Logger log = Logger.getLogger(DistributedTxnPreparedProcessorTimerTask.class.getName());

    public DistributedTxnPreparedProcessorTimerTask(TransactionProcessor processor) {
        this.processor = processor;
    }

    @Override
    public void run() {
        try {
            if (processor.getRemotePreparedListSize() > 0)
                processor.startCommitProcessForPreparedTransactions(null, Bzs.TransactionStatus.PREPARED);
        } catch (Exception e) {
            log.log(Level.WARNING,
                    "Exception occurred while running scheduled task to process remote prepared transactions: " + e.getLocalizedMessage(), e);
        }
    }
}
