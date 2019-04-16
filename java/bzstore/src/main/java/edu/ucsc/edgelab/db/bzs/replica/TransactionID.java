package edu.ucsc.edgelab.db.bzs.replica;



class TransactionID extends TuplePair implements Comparable<TransactionID>{

    public TransactionID(final int epochNumber, final int sequenceNumber) {
        super(epochNumber, sequenceNumber);
    }

    public Integer getEpochNumber() {
        return first;
    }
    public Integer getSequenceNumber() {
        return second;
    }

    @Override
    public int compareTo(TransactionID t2) {
        int eDiff = first - t2.first;
        if (eDiff!=0) {
            return eDiff;
        }
        return second - t2.second;
    }

    @Override
    public String toString() {
        return String.format("(Epoch, Sequence)=(%d,%d)",first,second);
    }
}
