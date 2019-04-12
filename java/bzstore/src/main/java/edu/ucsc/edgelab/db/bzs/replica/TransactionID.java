package edu.ucsc.edgelab.db.bzs.replica;



class TransactionID extends TuplePair implements Comparable<TransactionID>{

    public TransactionID(int epochNumber, int sequenceNumber) {
        super(epochNumber, sequenceNumber);
    }

    @Override
    public int compareTo(TransactionID t2) {
        int eDiff = first - t2.first;
        if (eDiff!=0) {
            return eDiff;
        }
        return second - t2.second;
    }
}
