package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.bftcommit.BFTClient;
import edu.ucsc.edgelab.db.bzs.data.TransactionCache;
import edu.ucsc.edgelab.db.bzs.replica.DependencyVectorManager;
import edu.ucsc.edgelab.db.bzs.replica.ID;
import edu.ucsc.edgelab.db.bzs.replica.SmrLog;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

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

        Set<Bzs.Transaction> allRWT = new LinkedHashSet<>();
        Set<Bzs.Transaction> lRWTxns = new LinkedHashSet<>();

        for (int i = 0; i <= txnCount; i++) {
            TransactionID tid = new TransactionID(epochNumber, i);

            if (tid != null) {
                Bzs.Transaction rwt = TransactionCache.getTransaction(tid);
                if (rwt != null) {
                    MetaInfo metaInfo = localDataVerifier.getMetaInfo(rwt);
                    if (metaInfo.remoteRead || metaInfo.remoteWrite) {
                        dRWT.add(tid);
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

        // Send dRWT for remote prepare

        Map<Integer, List<Bzs.Transaction>> clusterDRWTMap = mapTransactionsToCluster(dRWT);

        // BFT Local Prepare everything

        final String batchID = String.format("%d:%d", ID.getClusterID(), epochNumber);
        final Bzs.TransactionBatch rwtLocalBatch = TxnUtils.getTransactionBatch(batchID, allRWT);
        Bzs.TransactionBatchResponse response = BFTClient.getInstance().performCommitPrepare(rwtLocalBatch);
        if (response!=null) {

        } else {
            // Send abort to all clients requests part of this batch. Send abort to all clusters involved in dRWT.
        }
        SmrLog.setLockLCEForEpoch(epochNumber);
        // Create SMR log entry. Including committed dRWTs, dvec, lce and perform a consensus on the SMR Log Entry.

        SmrLog.localPrepared(epochNumber, lRWT);
        SmrLog.distributedPrepared(epochNumber,dRWT);
        SmrLog.dependencyVector(epochNumber, DependencyVectorManager.getCurrentTimeVector());
        // If successful, Commit SMR Entry log. via BFT commit.
    }

    private Map<Integer, List<Bzs.Transaction>> mapTransactionsToCluster(Set<TransactionID> dRWTs) {
        Map<Integer, List<Bzs.Transaction>> tMap = new TreeMap<>();
        for (TransactionID dRWTid : dRWTs) {
            Bzs.Transaction drwt = TransactionCache.getTransaction(dRWTid);
            if (drwt != null) {
                Set<Integer> cids = TxnUtils.getListOfClusterIDs(drwt, ID.getClusterID());
                for (Integer cid: cids) {
                    if (!tMap.containsKey(cid)) {
                        tMap.put(cid, new LinkedList<>());
                    }
                    tMap.get(cid).add(drwt);
                }
            }
        }
        return tMap;
    }

    @Override
    public void run() {
        processEpoch();
    }
}
