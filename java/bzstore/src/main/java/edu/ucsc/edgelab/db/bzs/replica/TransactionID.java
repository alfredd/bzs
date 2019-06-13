package edu.ucsc.edgelab.db.bzs.replica;



public class TransactionID extends TuplePair implements Comparable<TransactionID>{

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
    public boolean equals(Object tid) {
        TransactionID otherTid = ((TransactionID) tid);
        return this.first==otherTid.first && this.second==otherTid.second;
    }

    public String getTiD () {
        return String.format("%d:%d",first,second);
    }

    public static final TransactionID getTransactionID(String tid) {
        String[] tidParts = tid.split(":");
        return new TransactionID(Integer.decode(tidParts[0]),Integer.decode(tidParts[1]));
    }
    @Override
    public String toString() {
        return String.format("((Epoch, Sequence)=(%d,%d))",first,second);
    }

    @Override
    public int hashCode() {
        return this.getTiD().hashCode();
    }
}
