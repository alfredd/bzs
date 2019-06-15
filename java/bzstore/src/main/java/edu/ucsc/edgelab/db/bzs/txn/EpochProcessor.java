package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.TransactionCache;
import edu.ucsc.edgelab.db.bzs.replica.ID;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;

import java.util.LinkedList;
import java.util.List;

public class EpochProcessor {

    private LocalDataVerifier localDataVerifier = new LocalDataVerifier(ID.getClusterID());

    private List<TransactionID> lRWT = new LinkedList<>();
    private List<TransactionID> dRWT = new LinkedList<>();
    private Integer epochNumber;

    public EpochProcessor(Integer epochNumber) {
        this.epochNumber = epochNumber;
    }

    public void processEpoch(final Integer sequence) {
        List<Bzs.Transaction> allRWT = new LinkedList<>();
        for (int i =0;i<=sequence;i++) {
            TransactionID tid = new TransactionID(epochNumber, i);

            if (tid != null) {
                Bzs.Transaction rwt = TransactionCache.getTransaction(tid);
                MetaInfo metaInfo = localDataVerifier.getMetaInfo(rwt);
                if (metaInfo.remoteRead || metaInfo.remoteWrite) {
                    dRWT.add(tid);
                } else {
                    lRWT.add(tid);
                }
                allRWT.add(rwt);
            }
        }

        // Send dRWT for prepare

        // BFT Prepare everything

        // BFT Commit lRWT

        // BFT add to SMR log
    }
}
