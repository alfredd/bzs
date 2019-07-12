package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.LockManager;
import edu.ucsc.edgelab.db.bzs.data.TransactionCache;
import edu.ucsc.edgelab.db.bzs.replica.ID;
import edu.ucsc.edgelab.db.bzs.replica.PerformanceTrace;
import edu.ucsc.edgelab.db.bzs.replica.Serializer;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;
import io.grpc.stub.StreamObserver;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TxnProcessor {

    private Serializer serializer;
    private EpochManager epochManager;
    public static final Logger log = Logger.getLogger(TxnProcessor.class.getName());
    private LocalDataVerifier localDataVerifier;
    private final PerformanceTrace performanceTracer;

    public TxnProcessor() {
        serializer = new Serializer();
        epochManager = new EpochManager();
        epochManager.setSerializer(serializer);
        localDataVerifier = new LocalDataVerifier(ID.getClusterID());
        performanceTracer = new PerformanceTrace();
    }

    public void processTransaction(final Bzs.Transaction request, final StreamObserver<Bzs.TransactionResponse> responseObserver) {
        synchronized (this) {
            log.info(String.format("Received transaction request: %s", request.toString()));
            if (!serializer.serialize(request)) {
                log.info("Transaction cannot be serialized. Will abort. Request: " + request);
                Bzs.TransactionResponse response =
                        Bzs.TransactionResponse.newBuilder().setStatus(Bzs.TransactionStatus.ABORTED).build();
                if (responseObserver != null) {
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                } else {
                    log.log(Level.WARNING, "Transaction aborted: " + request.toString());
                }
                return;
            }

            MetaInfo meta = localDataVerifier.getMetaInfo(request);
            if (meta.remoteWrite)
                LockManager.acquireLocks(request);
            final TransactionID tid = epochManager.getTID();
            Bzs.Transaction transaction = Bzs.Transaction.newBuilder(request).setTransactionID(tid.getTiD()).build();
            TransactionCache.add(tid, transaction, responseObserver);
        }
    }

    public void prepareTransactionBatch(ClusterDRWTProcessor clusterDRWTProcessor) {
        Set<Bzs.Transaction> txnsToPrepare = commonRemoteTxnProcessCode(clusterDRWTProcessor, true);
        epochManager.clusterPrepare(txnsToPrepare, clusterDRWTProcessor);

    }

    private Set<Bzs.Transaction> commonRemoteTxnProcessCode(ClusterDRWTProcessor clusterDRWTProcessor, boolean acquireLocks) {
        Set<Bzs.Transaction> txnsToProcess = new LinkedHashSet<>();
        for (Bzs.Transaction t : clusterDRWTProcessor.getRequest().getTransactionsList()) {
            if (!serializer.serialize(t)) {
                clusterDRWTProcessor.addToFailedList(t);
            } else {

                txnsToProcess.add(t);
                if (acquireLocks)
                    LockManager.acquireLocks(t);
            }
        }
        return txnsToProcess;
    }

    public void commitTransactionBatch(ClusterDRWTProcessor clusterDRWTProcessor) {
        Set<Bzs.Transaction> txnsToPrepare = new LinkedHashSet<>();
        txnsToPrepare.addAll(clusterDRWTProcessor.getRequest().getTransactionsList());
        epochManager.clusterCommit(txnsToPrepare, clusterDRWTProcessor);
    }
}
