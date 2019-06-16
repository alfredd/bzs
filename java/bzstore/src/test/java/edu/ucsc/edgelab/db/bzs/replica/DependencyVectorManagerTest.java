package edu.ucsc.edgelab.db.bzs.replica;

import org.junit.Test;

import java.util.LinkedList;

import static org.junit.Assert.*;

public class DependencyVectorManagerTest {

    @Test
    public void setValueTest() {
        DependencyVectorManager.setValue(4,5);
        LinkedList<Integer> vec = DependencyVectorManager.getDependencyVector();
        assertEquals(5, vec.size());
        assertEquals(5, vec.get(4).intValue());

        DependencyVectorManager.setValue(10, 3);
        vec = DependencyVectorManager.getDependencyVector();
        assertEquals(11, vec.size());
        assertEquals(3, vec.get(10).intValue());

    }
}