package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.bftcommit.BFTClient;
import edu.ucsc.edgelab.db.bzs.data.TransactionCache;
import edu.ucsc.edgelab.db.bzs.replica.DependencyVectorManager;
import edu.ucsc.edgelab.db.bzs.replica.ID;
import edu.ucsc.edgelab.db.bzs.replica.SmrLog;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;
import io.grpc.stub.StreamObserver;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.ucsc.edgelab.db.bzs.Bzs.*;
import static edu.ucsc.edgelab.db.bzs.txn.TxnUtils.mapTransactionsToCluster;

public class EpochProcessor implements Runnable {

    private final Integer txnCount;
    private static final Logger log = Logger.getLogger(EpochProcessor.class.getName());
    private final WedgeDBThreadPoolExecutor threadPoolExecutor;
    private LocalDataVerifier localDataVerifier = new LocalDataVerifier(ID.getClusterID());

    private final Integer epochNumber;

    public EpochProcessor(Integer epochNumber, Integer txnCount, WedgeDBThreadPoolExecutor threadPoolExecutor) {
        this.epochNumber = epochNumber;
        this.txnCount = txnCount;
        this.threadPoolExecutor = threadPoolExecutor;
    }

    public void processEpoch() {
        SmrLog.createLogEntry(epochNumber);
        Epoch.setEpochUnderExecution(epochNumber);

        Map<TransactionID, Transaction> allRWT = new LinkedHashMap<>();
        Map<TransactionID, Transaction> lRWTxns = new LinkedHashMap<>();
        Map<TransactionID, Transaction> dRWTxns = new LinkedHashMap<>();

        for (int i = 0; i <= txnCount; i++) {
            TransactionID tid = new TransactionID(epochNumber, i);

            if (tid != null) {
                Transaction rwt = TransactionCache.getTransaction(tid);
                if (rwt != null) {
                    MetaInfo metaInfo = localDataVerifier.getMetaInfo(rwt);
                    if (metaInfo.remoteRead || metaInfo.remoteWrite) {
//                        dRWT.add(tid);
                        dRWTxns.put(tid, rwt);
                    } else {
//                        lRWT.add(tid);
                        lRWTxns.put(tid, rwt);
                    }
                    allRWT.put(tid, rwt);
                } else {
                    log.log(Level.WARNING, "Transaction with TID" + tid + ", not found in transaction cache.");
                }
            }
        }

        Map<Integer, Map<TransactionID, Transaction>> clusterDRWTMap = mapTransactionsToCluster(dRWTxns, ID.getClusterID());
        for (Map.Entry<Integer, Map<TransactionID, Transaction>> entry : clusterDRWTMap.entrySet()) {
            DRWTProcessor drwtProcessor = new DRWTProcessor(epochNumber, entry.getKey(), entry.getValue());
            threadPoolExecutor.addToConcurrentQueue(drwtProcessor);
        }

        // BFT Local Prepare everything
        final String batchID = epochNumber.toString();
        final TransactionBatch allRWTxnLocalBatch = TxnUtils.getTransactionBatch(batchID, allRWT.values(), Bzs.Operation.BFT_PREPARE);
        TransactionBatchResponse response = BFTClient.getInstance().performCommitPrepare(allRWTxnLocalBatch);
        if (response != null) {
            for (TransactionResponse txnResponse : response.getResponsesList()) {
                TransactionStatus respStatus = txnResponse.getStatus();
                TransactionID transactionID = TransactionID.getTransactionID(txnResponse.getTransactionID());
                switch (respStatus) {
                    case ABORTED:
                        StreamObserver<TransactionResponse> responseObserver = TransactionCache.getObserver(transactionID);
                        responseObserver.onNext(txnResponse);
                        responseObserver.onCompleted();
                        if (lRWTxns.containsKey(transactionID))
                            lRWTxns.remove(transactionID);
                        if (dRWTxns.containsKey(transactionID))
                            dRWTxns.remove(transactionID);
                        allRWT.remove(transactionID);
                        break;
                    case PREPARED:
//                        Map<TransactionID, Transaction> tempMap = lRWTxns;
//                        if (dRWTxns.containsKey(transactionID))
//                            tempMap = dRWTxns;
//                        updateVersion(tempMap, transactionID, txnResponse);
                }
            }
        } else {
            // Send abort to all clients requests part of this batch. Send abort to all clusters involved in dRWT.
        }
        // Create SMR log entry. Including committed dRWTs, dvec, lce and perform a consensus on the SMR Log Entry.

        SmrLog.localPrepared(epochNumber, lRWTxns.values());
        SmrLog.distributedPrepared(epochNumber, dRWTxns.values());
        SmrLog.setLockLCEForEpoch(epochNumber);
        SmrLog.updateLastCommittedEpoch(epochNumber);
        SmrLog.dependencyVector(epochNumber, DependencyVectorManager.getCurrentTimeVector());
        int status = -1;

        // Generate SMR log entry.
        SmrLogEntry logEntry = SmrLog.generateLogEntry(epochNumber);


        // Perform BFT Consensus on the SMR Log entry
        // TODO: Implementation
        status = BFTClient.getInstance().prepareSmrLogEntry(logEntry);
        if (status < 0) {
            log.log(Level.SEVERE, "FAILURE in BFT consensus to add entry to SMR log for epoch = %d.");
        }
        // Commit SMR log entry
        // TODO: Implementation
        BFTClient.getInstance().commitSMR(epochNumber);

    }

    @Override
    public void run() {
        processEpoch();
    }
}
