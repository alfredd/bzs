package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.bftcommit.BFTClient;
import edu.ucsc.edgelab.db.bzs.data.LockManager;
import edu.ucsc.edgelab.db.bzs.data.TransactionCache;
import edu.ucsc.edgelab.db.bzs.replica.*;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
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
    private Map<String, ClusterPC> clusterPrepareMap;
    private Map<String, ClusterPC> clusterCommitMap;

    public EpochProcessor(Integer epochNumber, Integer txnCount, WedgeDBThreadPoolExecutor threadPoolExecutor) {
        this.epochNumber = epochNumber;
        this.txnCount = txnCount;
        this.threadPoolExecutor = threadPoolExecutor;
        clusterPrepareMap = new LinkedHashMap<>();
        clusterCommitMap = new LinkedHashMap<>();
    }

    public void processEpoch() {
        SmrLog.createLogEntry(epochNumber);
        Epoch.setEpochUnderExecution(epochNumber);
        DependencyVectorManager.setValue(ID.getClusterID(), epochNumber);

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
                    log.log(Level.WARNING, "Transaction with TID" + tid + ", not found in transaction inProgressTxnMap.");
                }
            }
        }


        // BFT Local Prepare everything
        final String batchID = epochNumber.toString();
        TransactionBatch allRWTxnLocalBatch = TxnUtils.getTransactionBatch(batchID, allRWT.values(), Bzs.Operation.BFT_PREPARE);

        // Add 2PC remote transactions part of prepare batch.
        if (clusterPrepareMap.size() > 0) {
            for (Map.Entry<String, ClusterPC> pc : clusterPrepareMap.entrySet()) {
                Bzs.ClusterPC clusterPC = Bzs.ClusterPC.newBuilder()
                        .addAllTransactions(pc.getValue().batch)
                        .setOperation(Operation.DRWT_PREPARE)
                        .setID(pc.getKey())
                        .build();
                allRWTxnLocalBatch = TransactionBatch.newBuilder(allRWTxnLocalBatch).addRemotePrepareTxn(clusterPC).build();
            }
        }

        TransactionBatchResponse response = BFTClient.getInstance().performCommitPrepare(allRWTxnLocalBatch);
        if (response != null) {
            log.info(String.format("BFT Prepare response: %s", response.toString()));
            for (TransactionResponse txnResponse : response.getResponsesList()) {
                TransactionStatus respStatus = txnResponse.getStatus();
                TransactionID transactionID = TransactionID.getTransactionID(txnResponse.getTransactionID());
                if (respStatus != TransactionStatus.PREPARED) {
                        TxnUtils.sendAbortToClient(txnResponse, transactionID);
                        Transaction txn = TransactionCache.getTransaction(transactionID);
                        LockManager.releaseLocks(txn);
                        if (lRWTxns.containsKey(transactionID))
                            lRWTxns.remove(transactionID);
                        if (dRWTxns.containsKey(transactionID))
                            dRWTxns.remove(transactionID);
                        allRWT.remove(transactionID);
                }
            }
            if (response.getRemotePrepareTxnResponseCount() > 0) {
                for (ClusterPCResponse responseClusterPC : response.getRemotePrepareTxnResponseList()) {
                    ClusterPC cpc = clusterPrepareMap.get(responseClusterPC.getID());
                    Set<TransactionID> preparedTIDs = new LinkedHashSet<>();
                    for (TransactionResponse txnResponse : responseClusterPC.getResponsesList()) {
                        cpc.callback.addProcessedResponse(txnResponse);
                        if (txnResponse.getStatus().equals(TransactionStatus.PREPARED)) {
                            preparedTIDs.add(TransactionID.getTransactionID(txnResponse.getTransactionID()));
                        }
                    }
                    SmrLog.twoPCPrepared(epochNumber, cpc.batch, cpc.callback.getID());
                    RemoteTxnCache.addTIDsToPreparedBatch(responseClusterPC.getID(), preparedTIDs);
                    cpc.callback.setPreparedEpoch(epochNumber);
                    cpc.callback.setDepVector(DependencyVectorManager.getCurrentTimeVectorAsMap());
                    cpc.callback.sendResponseToClient();
                }
            }
        } else {
            // Send abort to all clients requests part of this batch. Send abort to all clusters involved in dRWT.
        }

        Map<Integer, Map<TransactionID, Transaction>> clusterDRWTMap = mapTransactionsToCluster(dRWTxns, ID.getClusterID());
        for (Map.Entry<Integer, Map<TransactionID, Transaction>> entry : clusterDRWTMap.entrySet()) {
            DRWTProcessor drwtProcessor = new DRWTProcessor(epochNumber, entry.getKey(), entry.getValue());
            threadPoolExecutor.addToConcurrentQueue(drwtProcessor);
        }
        DTxnCache.addToInProgressQueue(epochNumber, dRWTxns);

        int epochLCE = -1;
        if (clusterCommitMap.size() > 0) {
            for (Map.Entry<String, ClusterPC> cpcEntry : clusterCommitMap.entrySet()) {
                ClusterPC cpc = cpcEntry.getValue();
                String id = cpcEntry.getKey();
                Set<Transaction> prepared2PCTxns = new LinkedHashSet<>();
                for (Transaction t : cpc.batch) {
                    if (RemoteTxnCache.isTIDInPreparedBatch(id, TransactionID.getTransactionID(t.getTransactionID()))) {
                        prepared2PCTxns.add(t);
                    } else {
                        cpc.callback.addToFailedList(t);
                    }
                    int cpcPreparedEpoch = cpc.callback.getPreparedEpoch();
                    if (epochLCE < cpcPreparedEpoch)
                        epochLCE = cpcPreparedEpoch;
                }
                SmrLog.twoPCCommitted(epochNumber, prepared2PCTxns, id);
            }
        }
        SmrLog.updateLastCommittedEpoch(epochLCE);

        // Create SMR log entry. Including committed dRWTs, dvec, lce and perform a consensus on the SMR Log Entry.

        SmrLog.localPrepared(epochNumber, lRWTxns.values());
        SmrLog.distributedPrepared(epochNumber, dRWTxns.values());
        SmrLog.setLockLCEForEpoch(epochNumber);
        SmrLog.updateLastCommittedEpoch(epochNumber);
        SmrLog.committedDRWT(DTxnCache.getCommittedTransactions());
        SmrLog.dependencyVector(epochNumber, DependencyVectorManager.getCurrentTimeVector());
        int status = -1;

        // Generate SMR log entry.
        SmrLogEntry logEntry = SmrLog.generateLogEntry(epochNumber);


        // Perform BFT Consensus on the SMR Log entry
        status = BFTClient.getInstance().prepareSmrLogEntry(logEntry);
        if (status < 0) {
            log.log(Level.SEVERE, "FAILURE in BFT consensus to add entry to SMR log for epoch "+epochNumber);
        } else {
            // Commit SMR log entry
            BFTClient.getInstance().commitSMR(epochNumber);
            log.info(String.format("SMR log #%d: %s", epochNumber.intValue(), logEntry));
        }
    }

    @Override
    public void run() {
        processEpoch();
    }

    public void addClusterPrepare(final LinkedBlockingQueue<ClusterPC> clusterPrepareBatch) {
        log.info("Adding clusterPrepareBatch to prepare map: " + clusterPrepareBatch);
        for (ClusterPC cpc : clusterPrepareBatch) {
            clusterPrepareMap.put(cpc.callback.getID(), cpc);
        }
    }

    public void addClusterCommit(LinkedBlockingQueue<ClusterPC> clusterCommitBatch) {
        for (ClusterPC cpc : clusterCommitBatch) {
            clusterCommitMap.put(cpc.callback.getID(), cpc);
        }

    }
}

