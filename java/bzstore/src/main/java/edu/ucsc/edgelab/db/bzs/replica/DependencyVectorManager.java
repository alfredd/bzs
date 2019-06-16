package edu.ucsc.edgelab.db.bzs.replica;

import java.util.LinkedList;
import java.util.List;

public class DependencyVectorManager {

    private static List<Integer> dvec = new LinkedList<>();

    public static void setValue(Integer clusterID, Integer timestamp) {
        synchronized (dvec) {
            if (clusterID > (dvec.size() - 1)) {
                int count = clusterID - dvec.size();
                while (count >= 0)
                    dvec.add(0);
            }
            dvec.set(clusterID, timestamp);
        }
    }

}