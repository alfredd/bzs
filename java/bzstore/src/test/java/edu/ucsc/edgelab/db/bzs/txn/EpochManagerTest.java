package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;
import org.junit.Ignore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EpochManagerTest {

    @Ignore
    public void testGetTID() {
        EpochManager e = new EpochManager();
        TransactionID tid1 = e.getTID();
        TransactionID tid2 = e.getTID();
        assertTrue(tid1.compareTo(tid2) < 0);
        TransactionID tid3 = e.getTID();
        assertEquals(-1, tid1.compareTo(tid2));
        assertEquals(-2, tid1.compareTo(tid3));

        assertEquals(2, tid3.compareTo(tid1));

    }

    @Ignore
    public void testUpdateEpoch() {
        final int[] txnCounter = {0};
//        EpochManager e = new EpochManager() {
//
//            @Override
//            protected void processEpoch(Integer epoch, Integer txnCount) {
//                txnCounter[0] = txnCount;
//            }
//        };
        MockEpochManager e = new MockEpochManager();
        final long epochStartTime = System.currentTimeMillis();
        int seq = -1;
        int startEpoch = e.getTID().getEpochNumber();
        final TransactionID tid = e.getTID();
        Integer newEpochNumber = tid.getEpochNumber();
        seq=tid.getSequenceNumber();
        Integer newSeqNumber = 0;
//        System.out.println(String.format("Start Epoch Number = %d, New Epoch Number = %d", startEpoch, newEpochNumber.intValue()));
        while (newEpochNumber==startEpoch) {
            final TransactionID tid1 = e.getTID();
            newEpochNumber = tid1.getEpochNumber();
            if (newEpochNumber==startEpoch)
                seq = tid1.getSequenceNumber();
            else newSeqNumber = tid1.getSequenceNumber();
        }

        final long duration = System.currentTimeMillis() - epochStartTime;
//        System.out.println(duration+", "+ seq + ", "+e.txnCounter+", "+ newSeqNumber);

        assertEquals(0, newSeqNumber.intValue());
        assertTrue(seq >= Configuration.MAX_EPOCH_TXN || duration >= Configuration.MAX_EPOCH_DURATION_MS);
        if (seq >= Configuration.MAX_EPOCH_TXN)
            assertEquals(seq+EpochManager.EPOCH_BUFFER, e.txnCounter);
    }
}

class MockEpochManager extends EpochManager {
    public int txnCounter;
    @Override
    protected void processEpoch(Integer epoch, Integer txnCount) {
        txnCounter = txnCount;
    }
}