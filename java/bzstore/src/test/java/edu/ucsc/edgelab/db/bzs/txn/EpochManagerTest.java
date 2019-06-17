package edu.ucsc.edgelab.db.bzs.txn;

import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;
import org.junit.Test;

import static org.junit.Assert.*;

public class EpochManagerTest {

    @Test
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

    @Test
    public void testEpochTime() {
        EpochManager e = new EpochManager();
        final long epochStartTime = System.currentTimeMillis();
        e.setEpochStartTime(epochStartTime);

        assertEquals(epochStartTime, e.getEpochStartTime());
    }

    @Test
    public void testUpdateEpoch() {
        EpochManager e = new EpochManager();
        final long epochStartTime = System.currentTimeMillis();
        e.setEpochStartTime(epochStartTime);
        int seq = -1;
        int startEpoch = e.getTID().getEpochNumber();
        final TransactionID tid = e.getTID();
        Integer newEpochNumber = tid.getEpochNumber();
        seq=tid.getSequenceNumber();
        while (newEpochNumber==startEpoch) {
            final TransactionID tid1 = e.getTID();
            newEpochNumber = tid1.getEpochNumber();
            if (newEpochNumber==startEpoch)
                seq = tid1.getSequenceNumber();
        }

        long epochEndTime = System.currentTimeMillis();
        final long duration = epochEndTime - epochStartTime;
        System.out.println(duration+", "+ seq);

        assertTrue(seq >= Configuration.MAX_EPOCH_TXN || duration > Configuration.MAX_EPOCH_DURATION_MS);
    }
}