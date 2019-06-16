package edu.ucsc.edgelab.db.bzs.txn;

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
        while (true) {
            e.getTID();
            seq = e.updateEpoch();
            if (seq > 0) {
                break;
            }
        }

        long epochEndTime = System.currentTimeMillis();
        final long duration = epochEndTime - epochStartTime;

        assertTrue(seq >= 2000 || duration > 30000);
    }
}