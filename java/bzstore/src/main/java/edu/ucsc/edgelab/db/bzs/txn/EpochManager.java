package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;

import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Logger;

public class EpochManager {
    private volatile Integer epochNumber = 0;
    private volatile Integer sequenceNumber = 0;
    private EpochThreadPoolExecutor epochThreadPoolExecutor;
    public static final int EPOCH_BUFFER = 5;

    public static final Logger logger = Logger.getLogger(EpochManager.class.getName());


    public EpochManager() {
        epochThreadPoolExecutor = new EpochThreadPoolExecutor();
        TimerTask epochUpdater = new TimerTask() {

            @Override
            public void run() {
//                logger.info("Updating epoch.");
                updateEpoch();
//                logger.info("Epoch updated.");
            }
        };

        Timer t = new Timer();
        t.scheduleAtFixedRate(epochUpdater, Configuration.MAX_EPOCH_DURATION_MS, Configuration.MAX_EPOCH_DURATION_MS);
    }

    public TransactionID getTID() {
        synchronized (this) {
            final Integer epochNumber = this.epochNumber;
            final Integer sequenceNumber = this.sequenceNumber;
            this.sequenceNumber += 1;
            return new TransactionID(epochNumber, sequenceNumber);
        }
    }

    private Integer updateEpoch() {
        synchronized (this) {
            Integer seq = sequenceNumber;
            if (seq > 0) {
                final int epoch = epochNumber;
                seq = sequenceNumber-1;
                sequenceNumber = 0;
                epochNumber += 1;
                Epoch.setEpochNumber(epochNumber);
                processEpoch(epoch, seq+EPOCH_BUFFER);
            }
            return seq;
        }
    }

    protected void processEpoch(final Integer epoch, final Integer txnCount) {
        epochThreadPoolExecutor.addToThreadPool(new EpochProcessor(epoch, txnCount));
    }
}
