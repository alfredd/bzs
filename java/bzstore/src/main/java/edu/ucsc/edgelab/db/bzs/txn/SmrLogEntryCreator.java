package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;

import java.util.List;
import java.util.Set;

public class SmrLogEntryCreator {


    private Bzs.SmrLogEntry.Builder smrLogEntryBuilder;

    public SmrLogEntryCreator() {
        smrLogEntryBuilder = Bzs.SmrLogEntry.newBuilder();
    }

    public void setEpochNumber (int epoch) {
        smrLogEntryBuilder = smrLogEntryBuilder.setEpochNumber(epoch);
    }

    public void addDepVectorToSmrLog(List<Integer> dvec) {
        for (int i =0;i< dvec.size(); i++)
            smrLogEntryBuilder = smrLogEntryBuilder.putDepVector(i, dvec.get(i));
    }

    public void addCommittedlWRTxns(Set<Bzs.Transaction> lRWTxns) {
        for(Bzs.Transaction t : lRWTxns)
            smrLogEntryBuilder = smrLogEntryBuilder.addLRWTxns(t);
    }

    public void addPreparedDRWTxns(Set<Bzs.Transaction> dRWTxns) {
        for (Bzs.Transaction t: dRWTxns)
            smrLogEntryBuilder = smrLogEntryBuilder.addPreparedDRWTxns(t);
    }

    public void addCommittedDRWTxns(Set<Bzs.Transaction> dRWTxns) {
        for (Bzs.Transaction t: dRWTxns)
            smrLogEntryBuilder = smrLogEntryBuilder.addCommittedDRWTxns(t);
    }

    public void addLastCommittedEpoch(int lce) {
        smrLogEntryBuilder = smrLogEntryBuilder.setLce(lce);
    }

}
