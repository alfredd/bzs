package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.bftcommit.BFTClient;
import edu.ucsc.edgelab.db.bzs.data.TransactionCache;
import edu.ucsc.edgelab.db.bzs.replica.DependencyVectorManager;
import edu.ucsc.edgelab.db.bzs.replica.ID;
import edu.ucsc.edgelab.db.bzs.replica.SmrLog;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;
import io.grpc.stub.StreamObserver;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.ucsc.edgelab.db.bzs.Bzs.*;
import static edu.ucsc.edgelab.db.bzs.txn.TxnUtils.mapTransactionsToCluster;

public class EpochProcessor implements Runnable {

    private final Integer txnCount;
    private static final Logger log = Logger.getLogger(EpochProcessor.class.getName());
    private LocalDataVerifier localDataVerifier = new LocalDataVerifier(ID.getClusterID());

    private Set<TransactionID> lRWT = new LinkedHashSet<>();
    private Set<TransactionID> dRWT = new LinkedHashSet<>();
    private final Integer epochNumber;

    public EpochProcessor(Integer epochNumber, Integer txnCount) {
        this.epochNumber = epochNumber;
        this.txnCount = txnCount;
    }

    public void processEpoch() {
        SmrLog.createLogEntry(epochNumber);
        Epoch.setEpochUnderExecution(epochNumber);

        Set<Transaction> allRWT = new LinkedHashSet<>();
        Set<Transaction> lRWTxns = new LinkedHashSet<>();
        Set<Transaction> dRWTxns = new LinkedHashSet<>();

        for (int i = 0; i <= txnCount; i++) {
            TransactionID tid = new TransactionID(epochNumber, i);

            if (tid != null) {
                Transaction rwt = TransactionCache.getTransaction(tid);
                if (rwt != null) {
                    MetaInfo metaInfo = localDataVerifier.getMetaInfo(rwt);
                    if (metaInfo.remoteRead || metaInfo.remoteWrite) {
                        dRWT.add(tid);
                        dRWTxns.add(rwt);
                    } else {
                        lRWT.add(tid);
                        lRWTxns.add(rwt);
                    }
                    allRWT.add(rwt);
                } else {
                    log.log(Level.WARNING, "Transaction with TID" + tid + ", not found in transaction cache.");
                }
            }
        }

        // TODO Implementation:  Send dRWT for remote prepare
        Map<Integer, List<Transaction>> clusterDRWTMap = mapTransactionsToCluster(dRWT, ID.getClusterID());

        // BFT Local Prepare everything

        final String batchID = String.format("%d:%d", ID.getClusterID(), epochNumber);
        final TransactionBatch rwtLocalBatch = TxnUtils.getTransactionBatch(batchID, allRWT, Bzs.Operation.BFT_PREPARE);
        TransactionBatchResponse response = BFTClient.getInstance().performCommitPrepare(rwtLocalBatch);
        if (response != null) {
            for (TransactionResponse txnResponse : response.getResponsesList()) {
                TransactionStatus respStatus = txnResponse.getStatus();
                switch (respStatus) {
                    case ABORTED:
                        TransactionID transactionID = TransactionID.getTransactionID(txnResponse.getTransactionID());
                        StreamObserver<TransactionResponse> responseObserver = TransactionCache.getObserver(transactionID);
                        responseObserver.onNext(txnResponse);
                        responseObserver.onCompleted();
                        break;
                    case PREPARED:
                }
            }
        } else {
            // Send abort to all clients requests part of this batch. Send abort to all clusters involved in dRWT.
        }
        // Create SMR log entry. Including committed dRWTs, dvec, lce and perform a consensus on the SMR Log Entry.

        SmrLog.localPrepared(epochNumber, lRWTxns);
        SmrLog.distributedPrepared(epochNumber, dRWTxns);
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
