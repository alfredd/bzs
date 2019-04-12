package edu.ucsc.edgelab.db.bzs.replica;

public class TuplePair {
    public int n1;
    public int n2;

    public TuplePair(int n1, int n2) {
        this.n1 = n1;
        this.n2 = n2;
    }

    @Override
    public boolean equals(Object tid) {
        if (tid.getClass().getName().equals(TransactionID.class.getName())) {
            TransactionID objtid = (TransactionID) tid;
            return objtid.n1 == n1 && objtid.n2 == n2;
        }
        return false;

    }
}
