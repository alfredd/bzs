package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;
import edu.ucsc.edgelab.db.bzs.replica.Serializer;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class EpochManager {
    private volatile Integer epochNumber = 0;
    private volatile Integer sequenceNumber = 0;
    private WedgeDBThreadPoolExecutor epochThreadPoolExecutor;
    private WedgeDBThreadPoolExecutor dTxnThreadPoolExecutor;
    public static final int EPOCH_BUFFER = 5;

    private LinkedBlockingQueue<ClusterPC> clusterPrepareBatch = new LinkedBlockingQueue<>();

    public static final Logger logger = Logger.getLogger(EpochManager.class.getName());
    private Serializer serializer;


    public EpochManager() {
        epochThreadPoolExecutor = new WedgeDBThreadPoolExecutor();
        TimerTask epochUpdater = new TimerTask() {

            @Override
            public void run() {
//                logger.info("Updating epoch.");
                updateEpoch();
//                logger.info("Epoch updated.");
            }
        };
        epochNumber = BZDatabaseController.getEpochCount();
        Timer t = new Timer();
        t.scheduleAtFixedRate(epochUpdater, Configuration.MAX_EPOCH_DURATION_MS, Configuration.MAX_EPOCH_DURATION_MS);
        dTxnThreadPoolExecutor = new WedgeDBThreadPoolExecutor();
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
                serializer.resetEpoch();
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
        EpochProcessor processor = new EpochProcessor(epoch, txnCount, dTxnThreadPoolExecutor);
        processor.addClusterPrepare(clusterPrepareBatch);
        clusterPrepareBatch.clear();

        epochThreadPoolExecutor.addToFixedQueue(processor);
    }

    public void setSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

    public void clusterPrepare(Set<Bzs.Transaction> txnsToPrepare, ClusterDRWTProcessor clusterDRWTProcessor) {
        synchronized (this) {
            ClusterPC clusterPC = new ClusterPC();
            clusterPC.batch=txnsToPrepare;
            clusterPC.callback = clusterDRWTProcessor;
            clusterPrepareBatch.add(clusterPC);
        }
    }
}

class ClusterPC {
    Set<Bzs.Transaction> batch;
    ClusterDRWTProcessor callback;
}