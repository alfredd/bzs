package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;
import edu.ucsc.edgelab.db.bzs.replica.PerformanceTrace;
import edu.ucsc.edgelab.db.bzs.replica.Serializer;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;

import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class EpochManager {
    private final Integer maxEpochBatchSize;
    private volatile Integer epochNumber = 0;
    private volatile Integer sequenceNumber = 0;
    private WedgeDBThreadPoolExecutor epochThreadPoolExecutor;
    private WedgeDBThreadPoolExecutor dTxnThreadPoolExecutor;
    public static final int EPOCH_BUFFER = 5;

    private LinkedBlockingQueue<ClusterPC> clusterPrepareBatch = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<ClusterPC> clusterCommitBatch = new LinkedBlockingQueue<>();

    public static final Logger logger = Logger.getLogger(EpochManager.class.getName());
    private Serializer serializer;
    private PerformanceTrace perfTracer;


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
        maxEpochBatchSize = Configuration.getEpochBatchCount();
        epochNumber = BZDatabaseController.getEpochCount() + 1;
        Timer t = new Timer();
        t.scheduleAtFixedRate(epochUpdater, Configuration.getEpochTimeInMS(), Configuration.getEpochTimeInMS());
        dTxnThreadPoolExecutor = new WedgeDBThreadPoolExecutor();
    }

    public TransactionID getTID() {
        synchronized (this) {
            final Integer epochNumber = this.epochNumber;
            final Integer sequenceNumber = this.sequenceNumber;
            this.sequenceNumber += 1;
            if (sequenceNumber >= maxEpochBatchSize) {
                // Probable race condition.
                updateEpoch();
            }
            return new TransactionID(epochNumber, sequenceNumber);
        }
    }

    private Integer updateEpoch() {
        synchronized (this) {
            Integer seq = sequenceNumber;
            if (seq > 0 || clusterCommitBatch.size() > 0 || clusterPrepareBatch.size() > 0 || DTxnCache.completedDRWTxnsExist()) {
                final int epoch = epochNumber;
                logger.info("Processing Epoch: "+epoch);
                seq = sequenceNumber - 1;
                sequenceNumber = 0;
                epochNumber += 1;
                serializer.resetEpoch();
                Epoch.setEpochNumber(epochNumber);
                processEpoch(epoch, seq + EPOCH_BUFFER);
            }
            return seq;
        }
    }

    protected void processEpoch(final Integer epoch, final Integer txnCount) {
        EpochProcessor processor = new EpochProcessor(epoch, txnCount, dTxnThreadPoolExecutor);
        processor.addPerformanceTracer(perfTracer);
        processor.addClusterPrepare(clusterPrepareBatch);
        clusterPrepareBatch.clear();
        processor.addClusterCommit(clusterCommitBatch);
        clusterCommitBatch.clear();
        epochThreadPoolExecutor.addToFixedQueue(processor);
    }

    public void setSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

    public void clusterPrepare(Set<Bzs.Transaction> txnsToPrepare, ClusterDRWTProcessor clusterDRWTProcessor) {
        synchronized (this) {
            ClusterPC clusterPC = createClusterPCObj(txnsToPrepare, clusterDRWTProcessor);
            clusterPrepareBatch.add(clusterPC);
        }
    }

    private ClusterPC createClusterPCObj(Set<Bzs.Transaction> txns, ClusterDRWTProcessor clusterDRWTProcessor) {
        ClusterPC clusterPC = new ClusterPC();
        clusterPC.batch = txns;
        clusterPC.callback = clusterDRWTProcessor;
        return clusterPC;
    }

    public void clusterCommit(Set<Bzs.Transaction> txnsToCommit, ClusterDRWTProcessor clusterDRWTProcessor) {
        synchronized (this) {
            ClusterPC clusterPC = createClusterPCObj(txnsToCommit, clusterDRWTProcessor);
            clusterCommitBatch.add(clusterPC);
        }
    }

    public void setPerformanceTracer(PerformanceTrace performanceTracer) {
        this.perfTracer = performanceTracer;
    }
}

class ClusterPC {
    Set<Bzs.Transaction> batch;
    ClusterDRWTProcessor callback;
}