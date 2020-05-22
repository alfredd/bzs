package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.Bzs;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SmrLogEntryCreator {


    private Bzs.SmrLogEntry.Builder smrLogEntryBuilder;

    public SmrLogEntryCreator() {
        smrLogEntryBuilder = Bzs.SmrLogEntry.newBuilder();
    }

    public void setEpochNumber(final int epoch) {
        smrLogEntryBuilder = smrLogEntryBuilder.setEpochNumber(epoch);
    }

    public void addDepVectorToSmrLog(List<Integer> dvec) {
        for (int i = 0; i < dvec.size(); i++)
            smrLogEntryBuilder = smrLogEntryBuilder.putDepVector(i, dvec.get(i));
    }

    public void addDepVecMap(Map<Integer, Integer> depVecMap) {
        smrLogEntryBuilder = smrLogEntryBuilder.putAllDepVector(depVecMap);
    }

    public void addCommittedlWRTxns(Collection<Bzs.Transaction> lRWTxns) {
        for (Bzs.Transaction t : lRWTxns)
            smrLogEntryBuilder = smrLogEntryBuilder.addLRWTxns(t);
    }

    public void addPreparedDRWTxns(Collection<Bzs.Transaction> dRWTxns) {
        for (Bzs.Transaction t : dRWTxns)
            smrLogEntryBuilder = smrLogEntryBuilder.addPreparedDRWTxns(t);
    }

    public void add2PCPrepared(Collection<Bzs.Transaction> txns, String id) {
        Bzs.TwoPCDRWTxn entry = Bzs.TwoPCDRWTxn.newBuilder().addAllTransactions(txns).setID(id).build();
        smrLogEntryBuilder = smrLogEntryBuilder.addRemotePreparedDRWTs(entry);
    }

    public void add2PCCommitted(Set<Bzs.TransactionResponse> txns, String id) {
        Bzs.TwoPCDRWTxn entry = Bzs.TwoPCDRWTxn.newBuilder().addAllCommitted(txns).setID(id).build();
        smrLogEntryBuilder = smrLogEntryBuilder.addRemoteCommittedDRWTs(entry);
    }

    public void addCommittedDRWTxns(Bzs.TransactionResponse dRWTxns) {
        smrLogEntryBuilder = smrLogEntryBuilder.addCommittedDRWTxns(dRWTxns);
    }

    public void updateLastCommittedEpoch(int lce) {
        smrLogEntryBuilder = smrLogEntryBuilder.setLce(lce);
    }

    public Bzs.SmrLogEntry generateSmrLogEntry() {
        return smrLogEntryBuilder.build();
    }

}
