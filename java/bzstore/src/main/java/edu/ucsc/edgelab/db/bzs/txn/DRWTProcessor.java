package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;

import java.util.List;

public class DRWTProcessor implements Runnable{

    private final Integer cid;
    private final List<Bzs.Transaction> txns;

    public DRWTProcessor(Integer clusterID, List<Bzs.Transaction> transactions) {
        this.cid = clusterID;
        this.txns = transactions;
    }

    @Override
    public void run() {

    }
}
