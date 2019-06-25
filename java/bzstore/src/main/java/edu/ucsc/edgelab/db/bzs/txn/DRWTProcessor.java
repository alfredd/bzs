package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;

import java.util.Collection;
import java.util.Map;

public class DRWTProcessor implements Runnable{

    private final Integer cid;
    private final Map<TransactionID, Bzs.Transaction> txns;

    public DRWTProcessor(final Integer epochNumber, final Integer clusterID, final Map<TransactionID, Bzs.Transaction> transactions) {
        this.cid = clusterID;
        this.txns = transactions;
    }

    @Override
    public void run() {

    }
}
