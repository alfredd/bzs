package edu.ucsc.edgelab.db.bzs.replica;

public class TuplePair {
    public int first;
    public int second;

    public TuplePair(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean equals(Object tid) {
        if (tid.getClass().getName().equals(TransactionID.class.getName())) {
            TransactionID objtid = (TransactionID) tid;
            return objtid.first == first && objtid.second == second;
        }
        return false;

    }
}
