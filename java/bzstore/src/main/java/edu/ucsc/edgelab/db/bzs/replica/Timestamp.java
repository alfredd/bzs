package edu.ucsc.edgelab.db.bzs.replica;

import java.util.LinkedHashMap;
import java.util.Map;

public class Timestamp implements Comparable{
    private Map<Integer, Integer> timestamp = new LinkedHashMap<>();

    public void add(Integer clusterID, Integer epochNumber) {
        timestamp.put(clusterID, epochNumber);
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}
