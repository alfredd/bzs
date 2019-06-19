package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.txn.Epoch;
import org.junit.Test;

import java.util.LinkedList;

import static org.junit.Assert.assertEquals;

public class DependencyVectorManagerTest {

    @Test
    public void setValueTest() {
        Epoch.setEpochNumber(1);
        ID.setIDs(0,0);
        Epoch.setEpochUnderExecution(0);
        DependencyVectorManager.setValue(4,5);
        LinkedList<Integer> vec = DependencyVectorManager.getCurrentTimeVector();
        assertEquals(5, vec.size());
        assertEquals(5, vec.get(4).intValue());

        DependencyVectorManager.setValue(10, 3);
        vec = DependencyVectorManager.getCurrentTimeVector();
        assertEquals(11, vec.size());
        assertEquals(3, vec.get(10).intValue());

    }
}