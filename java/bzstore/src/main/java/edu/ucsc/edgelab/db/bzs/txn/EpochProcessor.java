package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.bftcommit.BFTClient;
import edu.ucsc.edgelab.db.bzs.data.LockManager;
import edu.ucsc.edgelab.db.bzs.data.TransactionCache;
import edu.ucsc.edgelab.db.bzs.performance.BatchMetricsManager;
import edu.ucsc.edgelab.db.bzs.performance.PerfMetricManager;
import edu.ucsc.edgelab.db.bzs.replica.*;
import io.grpc.stub.StreamObserver;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import static edu.ucsc.edgelab.db.bzs.Bzs.*;

public class EpochProcessor implements Runnable {

    protected final Integer txnCount;
    private static final Logger log = Logger.getLogger(EpochProcessor.class.getName());
    protected final WedgeDBThreadPoolExecutor threadPoolExecutor;
    protected LocalDataVerifier localDataVerifier = new LocalDataVerifier();



    private BatchMetricsManager batchMetricsManager;

    protected final Integer epochNumber;
    protected Map<String, ClusterPC> clusterPrepareMap;
    protected Map<String, ClusterPC> clusterCommitMap;
    protected PerformanceTrace perfTracer;
    protected PerfMetricManager perfLogger;

    public EpochProcessor(final Integer epochNumber, final Integer txnCount, WedgeDBThreadPoolExecutor threadPoolExecutor) {
        this.epochNumber = epochNumber;
        this.txnCount = txnCount;
        this.threadPoolExecutor = threadPoolExecutor;
        clusterPrepareMap = new LinkedHashMap<>();
        clusterCommitMap = new LinkedHashMap<>();
    }

    public void processEpoch() {
        long startTime = System.currentTimeMillis();
        TxnUtils txnUtils = new TxnUtils();
        // Perf trace
//        perfTracer.setBatchStartTime(epochNumber, startTime);

//        log.info("Processing Epoch #"+epochNumber);
        SmrLog.createLogEntry(epochNumber);

        Epoch.setEpochUnderExecution(epochNumber);
        DependencyVectorManager.setValue(ID.getClusterID(), epochNumber);

        Map<TransactionID, Transaction> allRWT = new LinkedHashMap<>();
        Map<TransactionID, Transaction> lRWTxns = new LinkedHashMap<>();
        Map<TransactionID, Transaction> dRWTxns = new LinkedHashMap<>();
        int actualTxnPrepareCount = 0;
        // Insert and set start time
        batchMetricsManager.setInitialBatchMetrics(epochNumber);
        for (int i = 0; i <= txnCount; i++) {
            TransactionID tid = new TransactionID(epochNumber, i);

            if (tid != null) {
                Transaction rwt = TransactionCache.getTransaction(tid);
                /**
                 * The next line logs the start time on the transaction observer.
                 */
                StreamObserver<TransactionResponse> observer = TransactionCache.getObserver(tid);
                if (observer!=null) {
                    observer.onNext(TransactionResponse.newBuilder().build());
                }




                if (rwt != null) {
                    MetaInfo metaInfo = localDataVerifier.getMetaInfo(rwt);
                    if (metaInfo.remoteRead || metaInfo.remoteWrite) {
                        dRWTxns.put(tid, rwt);
                        batchMetricsManager.incrementDRWT(epochNumber);
                    } else {
                        lRWTxns.put(tid, rwt);
                        batchMetricsManager.incrementLRWT(epochNumber);
                    }
                    allRWT.put(tid, rwt);
                    actualTxnPrepareCount += 1;
                } /*else {
                    log.log(Level.WARNING, "Transaction with TID" + tid + ", not found in transaction inProgressTxnMap.");
                }*/
            }
        }


        // BFT Local Prepare everything
        final String batchID = epochNumber.toString();
        TransactionBatch allRWTxnLocalBatch = txnUtils.getTransactionBatch(batchID, allRWT.values(), Bzs.Operation.BFT_PREPARE);

        // Add 2PC remote transactions part of prepare batch.
        if (clusterPrepareMap.size() > 0) {
            for (Map.Entry<String, ClusterPC> pc : clusterPrepareMap.entrySet()) {
                Bzs.ClusterPC clusterPC = Bzs.ClusterPC.newBuilder()
                        .addAllTransactions(pc.getValue().batch)
                        .setOperation(Operation.DRWT_PREPARE)
                        .setID(pc.getKey())
                        .build();
                actualTxnPrepareCount += pc.getValue().batch.size();
                allRWTxnLocalBatch = TransactionBatch.newBuilder(allRWTxnLocalBatch).addRemotePrepareTxn(clusterPC).build();
            }
        }

        TransactionBatchResponse transactionBatchResponse = null;

        if (actualTxnPrepareCount <= 0) {
            log.log(Level.WARNING, "No transactions found in this Epoch: " + epochNumber);
        } else {
//            log.info("Preparing transactions for epoch: "+epochNumber);
            transactionBatchResponse = BFTClient.getInstance().performCommitPrepare(allRWTxnLocalBatch);
            if (transactionBatchResponse != null) {
//                log.info(String.format("BFT Prepare transactionBatchResponse: %s", transactionBatchResponse.toString()));
                for (TransactionResponse txnResponse : transactionBatchResponse.getResponsesList()) {
                    TransactionStatus respStatus = txnResponse.getStatus();
                    TransactionID transactionID = TransactionID.getTransactionID(txnResponse.getTransactionID());
                    if (respStatus != TransactionStatus.PREPARED) {
                        txnUtils.sendAbortToClient(txnResponse, transactionID);
                        Transaction txn = TransactionCache.getTransaction(transactionID);
                        LockManager.releaseLocks(txn);
                        if (lRWTxns.containsKey(transactionID)) {
                            lRWTxns.remove(transactionID);
                        }
                        if (dRWTxns.containsKey(transactionID)) {
                            dRWTxns.remove(transactionID);
                            txnUtils.updateDRWTxnResponse(txnResponse, transactionID, ID.getClusterID());
                        }
                        allRWT.remove(transactionID);
                    }
                }
                if (transactionBatchResponse.getRemotePrepareTxnResponseCount() > 0) {
                    for (ClusterPCResponse responseClusterPC : transactionBatchResponse.getRemotePrepareTxnResponseList()) {
                        ClusterPC cpc = clusterPrepareMap.get(responseClusterPC.getID());
                        Set<TransactionID> preparedTIDs = new LinkedHashSet<>();
                        for (TransactionResponse txnResponse : responseClusterPC.getResponsesList()) {
                            cpc.callback.addProcessedResponse(txnResponse);

                            DependencyVectorManager.updateLocalClock(cpc.callback.getRequest().getDepVectorMap());

                            if (txnResponse.getStatus().equals(TransactionStatus.PREPARED)) {
                                preparedTIDs.add(TransactionID.getTransactionID(txnResponse.getTransactionID()));
                            }
                        }
                        SmrLog.twoPCPrepared(epochNumber, cpc.batch, cpc.callback.getID());
                        String id = responseClusterPC.getID();
//                        log.info("Adding prepared 2PC transactions to RemoteTxnCache: " + id + ", list of preparedTIDs: " + preparedTIDs);
                        RemoteTxnCache.addTIDsToPreparedBatch(id, preparedTIDs);
                        cpc.callback.setPreparedEpoch(epochNumber);
                        cpc.callback.setDepVector(DependencyVectorManager.getCurrentTimeVectorAsMap());
                        cpc.callback.sendResponseToClient();
                    }
                }
            } /*else {
                log.log(Level.WARNING, "Transaction batch response was null. Epoch: " + epochNumber);
                // Send abort to all clients requests part of this batch. Send abort to all clusters involved in dRWT.
            }*/

            Map<Integer, Map<TransactionID, Transaction>> clusterDRWTMap = txnUtils.mapTransactionsToCluster(dRWTxns, ID.getClusterID());
            for (Map.Entry<Integer, Map<TransactionID, Transaction>> entry : clusterDRWTMap.entrySet()) {
                log.info("Starting DRWTProcessor for cluster: " + entry.getKey() + ", and TIDs: " + entry.getValue().keySet());
                DRWTProcessor drwtProcessor = new DRWTProcessor(epochNumber, entry.getKey(), entry.getValue());
                drwtProcessor.setPerfMetricManager(perfLogger);
                threadPoolExecutor.addToConcurrentQueue(drwtProcessor);
            }
            if (dRWTxns.size() > 0)
                DTxnCache.addToInProgressQueue(epochNumber, dRWTxns);
        }
        long prepareTimeMS = System.currentTimeMillis() - startTime;
        int epochLCE = -1;
        if (clusterCommitMap.size() > 0) {
//            log.info("Creating BFT 2PC commit request in SMR log.");
            for (Map.Entry<String, ClusterPC> cpcEntry : clusterCommitMap.entrySet()) {
                ClusterPC cpc = cpcEntry.getValue();
                String id = cpcEntry.getKey();
//                log.info("2PC Transactions to commit: " + cpc.batch + ", ID: " + id);
                Set<Transaction> prepared2PCTxns = new LinkedHashSet<>();
                for (Transaction t : cpc.batch) {
                    TransactionID transactionID = TransactionID.getTransactionID(t.getTransactionID());
                    if (RemoteTxnCache.isTIDInPreparedBatch(id, transactionID)) {
//                        log.info("Found transaction in prepared batch: " + transactionID);
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
        SmrLog.updateEpochLCE(epochNumber, epochLCE);

        // Create SMR log entry. Including committed dRWTs, dvec, lce and perform a consensus on the SMR Log Entry.

        SmrLog.localPrepared(epochNumber, lRWTxns.values());
        SmrLog.distributedPrepared(epochNumber, dRWTxns.values());
        SmrLog.setLockLCEForEpoch(epochNumber);
        SmrLog.updateLastCommittedEpoch(epochNumber);
        final Collection<Transaction> committedTransactions = DTxnCache.getCommittedTransactions();
        SmrLog.committedDRWT(epochNumber, committedTransactions);
        SmrLog.dependencyVector(epochNumber, DependencyVectorManager.getCurrentTimeVector());

        // Generate SMR log entry.
        SmrLogEntry logEntry = SmrLog.generateLogEntry(epochNumber);


        // Perform BFT Consensus on the SMR Log entry
        long smrCommitStartTime = System.currentTimeMillis();
        int status = BFTClient.getInstance().prepareSmrLogEntry(logEntry);
        long duration = System.currentTimeMillis() - smrCommitStartTime;

        log.info("Time to prepare SMR log: " + (duration) + "ms.");
        TransactionStatus commitStatus = TransactionStatus.COMMITTED;
        if (status < 0) {
            log.log(Level.SEVERE, "FAILURE in BFT consensus to add entry to SMR log for epoch " + epochNumber);
            commitStatus = TransactionStatus.ABORTED;
        } else {
            // Commit SMR log entry
            BFTClient.getInstance().commitSMR(epochNumber);
//            log.info(String.format("SMR log committed #%d: %s", epochNumber.intValue(), logEntry));
        }


        // Send transactionBatchResponse to clients
        if (transactionBatchResponse != null) {
            for (TransactionResponse txnResponse : transactionBatchResponse.getResponsesList()) {
                String id = txnResponse.getTransactionID();
                TransactionID transactionID = TransactionID.getTransactionID(id);
                StreamObserver<TransactionResponse> responseObserver = TransactionCache.getObserver(transactionID);
                if (lRWTxns.containsKey(transactionID)) {
//                    log.info("Sending a Transaction Response for transaction with ID " + id + ": " + transactionBatchResponse);
                    if (responseObserver != null) {
                        TransactionResponse newResponse = TransactionResponse.newBuilder(txnResponse)
                                .putAllDepVector(DependencyVectorManager.getCurrentTimeVectorAsMap())
                                .setStatus(commitStatus)
                                .build();
                        responseObserver.onNext(newResponse);
                        responseObserver.onCompleted();
                        batchMetricsManager.setTxnCommitCompleted(transactionID);
                        batchMetricsManager.setTxnProcessingCompleted(transactionID);
                    } else {
//                        log.log(Level.WARNING,
//                                "Could not find appropriate transactionBatchResponse observer for transaction request: " +
// transactionBatchResponse);
                    }
                } else {
//                    log.info("Response to clients for transaction with ID " + id + " WILL NOT BE SENT as it is a DRWTxn. : " +
// transactionBatchResponse);
                }
            }
        }

        for (Transaction ct : committedTransactions) {
            TransactionID ctid = TransactionID.getTransactionID(ct.getTransactionID());
            batchMetricsManager.setTxnCommitCompleted(ctid);
            StreamObserver<TransactionResponse> observer = TransactionCache.getObserver(ctid);
            TransactionResponse response = TransactionCache.getResponse(ctid);
//            log.info("Sending a Transaction Response for transaction with ID " + ctid + ": " + response);
            observer.onNext(response);
            observer.onCompleted();
            LockManager.releaseLocks(ct);
        }

        if (clusterCommitMap.size() > 0) {
            for (Map.Entry<String, ClusterPC> cpcEntry : clusterCommitMap.entrySet()) {
                ClusterDRWTProcessor callback = cpcEntry.getValue().callback;
                callback.setDepVector(DependencyVectorManager.getCurrentTimeVectorAsMap());
                callback.sendResponseToClient();
            }
        }
        long processingTime = System.currentTimeMillis() - startTime;
//        String metrics = String.format("Epoch Metrics: Epoch # = %d,  Bytes = %d, Txn Count:#LRWT = %d, #DRWT = %d, Epoch pxng time = %dms"
//                , epochNumber.intValue()
//                , logEntry.toByteArray().length
//                , logEntry.getLRWTxnsCount()
//                , logEntry.getCommittedDRWTxnsCount()
//                , processingTime
//        );
        perfLogger.insertLocalPerfData(epochNumber,
                logEntry.getLRWTxnsCount(),
                logEntry.getPreparedDRWTxnsCount(),
                logEntry.getCommittedDRWTxnsCount(),
                logEntry.toByteArray().length,
                processingTime,
                prepareTimeMS,
                processingTime - prepareTimeMS);
        perfLogger.logMetrics(epochNumber);
        log.info("EPOCH PROCESSING TIME: "+(System.currentTimeMillis()-startTime));
//        log.info(metrics);
    }

    @Override
    public void run() {
        processEpoch();
    }

    public void addClusterPrepare(final LinkedBlockingQueue<ClusterPC> clusterPrepareBatch) {
//        log.info("Adding clusterPrepareBatch to prepare map: " + clusterPrepareBatch);
        for (ClusterPC cpc : clusterPrepareBatch) {
            clusterPrepareMap.put(cpc.callback.getID(), cpc);
        }
    }

    public void addClusterCommit(LinkedBlockingQueue<ClusterPC> clusterCommitBatch) {
//        log.info("Adding clusterCommitBatch to commit map: " + clusterCommitBatch);
        for (ClusterPC cpc : clusterCommitBatch) {
            clusterCommitMap.put(cpc.callback.getID(), cpc);
        }

    }

    public void addPerformanceTracer(PerformanceTrace perfTracer) {
        this.perfTracer = perfTracer;
    }

    public void setPerfMetricManager(PerfMetricManager perfMetricManager) {
        this.perfLogger = perfMetricManager;
    }

    public BatchMetricsManager getBatchMetricsManager() {
        return batchMetricsManager;
    }

    public void setBatchMetricsManager(BatchMetricsManager batchMetricsManager) {
        this.batchMetricsManager = batchMetricsManager;
    }
}

