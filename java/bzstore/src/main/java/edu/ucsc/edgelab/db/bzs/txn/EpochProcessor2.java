package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.TransactionCache;
import edu.ucsc.edgelab.db.bzs.replica.DependencyVectorManager;
import edu.ucsc.edgelab.db.bzs.replica.ID;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EpochProcessor2 extends EpochProcessor {

    private static final Logger log = Logger.getLogger(EpochProcessor2.class.getName());
    private Bzs.TransactionBatch.Builder txnBatchBuilder;
    private Map<Integer, Integer> depVector = new HashMap<>();

    public EpochProcessor2(Integer epochNumber, Integer txnCount, WedgeDBThreadPoolExecutor threadPoolExecutor) {
        super(epochNumber, txnCount, threadPoolExecutor);
        txnBatchBuilder = Bzs.TransactionBatch.newBuilder();
    }

    @Override
    public void processEpoch() {
        Epoch.setEpochUnderExecution(epochNumber);
        DependencyVectorManager.setValue(ID.getClusterID(), epochNumber);


//        Map<TransactionID, Bzs.Transaction> allRWT = new LinkedHashMap<>();
//        Map<TransactionID, Bzs.Transaction> lRWTxns = new LinkedHashMap<>();
//        Map<TransactionID, Bzs.Transaction> dRWTxns = new LinkedHashMap<>();
        int actualTxnPrepareCount = 0;
        for (int i = 0; i <= txnCount; i++) {
            TransactionID tid = new TransactionID(epochNumber, i);

            if (tid != null) {
                Bzs.Transaction rwt = TransactionCache.getTransaction(tid);
                if (rwt != null) {
                    MetaInfo metaInfo = localDataVerifier.getMetaInfo(rwt);
                    if (metaInfo.remoteRead || metaInfo.remoteWrite) {
                        txnBatchBuilder = txnBatchBuilder.addTxnBatch(
                                Bzs.ClusterPC.newBuilder()
                                        .addTransactions(rwt)
                                        .setOperation(Bzs.Operation.DRWT_PREPARE)
                                        .setID(tid.getTiD())
                                        .build()
                        );
//                        dRWTxns.put(tid, rwt);
                    } else {
                        txnBatchBuilder = txnBatchBuilder.addTxnBatch(
                                Bzs.ClusterPC.newBuilder()
                                        .addTransactions(rwt)
                                        .setOperation(Bzs.Operation.LOCAL_RWT)
                                        .setID(tid.getTiD())
                                        .build()
                        );
//                        lRWTxns.put(tid, rwt);
                    }
//                    allRWT.put(tid, rwt);
                    actualTxnPrepareCount += 1;
                } else {
                    log.log(Level.WARNING, "Transaction with TID" + tid + ", not found in transaction inProgressTxnMap.");
                }
            }
        }

        add2PCTxnsToBatch(clusterPrepareMap, Bzs.Operation.TWO_PC_PREPARE, actualTxnPrepareCount);
        add2PCTxnsToBatch(clusterCommitMap, Bzs.Operation.TWO_PC_COMMIT, actualTxnPrepareCount);
        txnBatchBuilder = txnBatchBuilder.setOperation(Bzs.Operation.BFT_PREPARE);

        Bzs.TransactionBatch transactionBatch = txnBatchBuilder.build();


    }

    private void add2PCTxnsToBatch(Map<String, ClusterPC> clusterPCMap,
                                   Bzs.Operation operation, int actualTxnPrepareCount) {
        if (clusterPCMap.size() > 0) {
            for (Map.Entry<String, ClusterPC> twoPCPrepareTxn : clusterPCMap.entrySet()) {

                Bzs.ClusterPC clusterPC = Bzs.ClusterPC.newBuilder()
                        .addAllTransactions(twoPCPrepareTxn.getValue().batch)
                        .setOperation(operation)
                        .setID(twoPCPrepareTxn.getKey())
                        .build();
                txnBatchBuilder = txnBatchBuilder.addTxnBatch(clusterPC);
                actualTxnPrepareCount += twoPCPrepareTxn.getValue().batch.size();

            }
        }
    }
}
