package edu.ucsc.edgelab.db.bzs.replica;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DependencyManager {
    private DependencyManager() {}

    private static Map<Integer, Timestamp> dependencyMap = new ConcurrentHashMap<>();

    public static void add(Integer clusterID, Timestamp clusterTimestamp) {
        dependencyMap.put(clusterID, clusterTimestamp);
    }

    public static Timestamp getEpochTimestamp (Integer epochNumber) {
        return dependencyMap.get(epochNumber);
    }

}


