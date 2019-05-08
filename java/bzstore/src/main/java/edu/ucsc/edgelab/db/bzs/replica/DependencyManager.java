package edu.ucsc.edgelab.db.bzs.replica;

import java.util.LinkedHashMap;
import java.util.Map;

public class DependencyManager {
    private Map<Integer, Timestamp> dependencyMap = new LinkedHashMap<>();

    public void add(Integer clusterID, Timestamp clusterTimestamp) {
        dependencyMap.put(clusterID, clusterTimestamp);
    }

}


