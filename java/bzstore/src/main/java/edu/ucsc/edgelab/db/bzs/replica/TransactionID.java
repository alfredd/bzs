package edu.ucsc.edgelab.db.bzs.replica;



class TransactionID extends TuplePair implements Comparable<TransactionID>{

    public TransactionID(int epochNumber, int sequenceNumber) {
        super(epochNumber, sequenceNumber);
    }

    @Override
    public int compareTo(TransactionID t2) {
        int eDiff = n1 - t2.n1;
        if (eDiff!=0) {
            return eDiff;
        }
        return n2 - t2.n2;
    }
}
