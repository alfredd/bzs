package edu.ucsc.edgelab.db.bzs.replica;

import java.util.LinkedHashMap;
import java.util.Map;

public class DependencyManager {
    private Map<Integer, Integer> dependencyMap;

    public DependencyManager() {
        dependencyMap = new LinkedHashMap<>();
    }
}
