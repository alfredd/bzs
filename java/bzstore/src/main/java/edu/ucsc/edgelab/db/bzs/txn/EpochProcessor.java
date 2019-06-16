package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.TransactionCache;
import edu.ucsc.edgelab.db.bzs.replica.ID;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EpochProcessor implements Runnable{

    private final Integer txnCount;
    private static final Logger log = Logger.getLogger(EpochProcessor.class.getName());
    private LocalDataVerifier localDataVerifier = new LocalDataVerifier(ID.getClusterID());

    private List<TransactionID> lRWT = new LinkedList<>();
    private List<TransactionID> dRWT = new LinkedList<>();
    private Integer epochNumber;

    public EpochProcessor(Integer epochNumber, Integer txnCount) {
        this.epochNumber = epochNumber;
        this.txnCount = txnCount;
    }

    public void processEpoch() {
        List<Bzs.Transaction> allRWT = new LinkedList<>();
        for (int i =0;i<=txnCount;i++) {
            TransactionID tid = new TransactionID(epochNumber, i);

            if (tid != null) {
                Bzs.Transaction rwt = TransactionCache.getTransaction(tid);
                if (rwt!=null) {
                    MetaInfo metaInfo = localDataVerifier.getMetaInfo(rwt);
                    if (metaInfo.remoteRead || metaInfo.remoteWrite) {
                        dRWT.add(tid);
                    } else {
                        lRWT.add(tid);
                    }
                    allRWT.add(rwt);
                } else {
                    log.log(Level.WARNING, "Transaction with TID"+ tid+", not found in transaction cache.");
                }
            }
        }

        // Send dRWT for remote prepare

        // BFT Local Prepare everything

        // BFT Commit lRWT

        // BFT add to SMR log
    }

    @Override
    public void run() {
        processEpoch();
    }
}
