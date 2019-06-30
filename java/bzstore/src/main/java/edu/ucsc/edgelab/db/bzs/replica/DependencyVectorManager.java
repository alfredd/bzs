package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.txn.Epoch;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class DependencyVectorManager {

    private static List<Integer> dvec = new LinkedList<>();

    public static void setValue(Integer clusterID, Integer timestamp) {
        synchronized (dvec) {
            if (clusterID > (dvec.size() - 1)) {
                int count = clusterID - dvec.size();
                while (count >= 0) {
                    dvec.add(-1);
                    count--;
                }
            }
            if (dvec.get(clusterID) < timestamp)
                dvec.set(clusterID, timestamp);
        }
    }

    public static LinkedList<Integer> getCurrentTimeVector() {
        synchronized (dvec) {
            int epoch = Epoch.getEpochUnderExecution();
            dvec.set(ID.getClusterID(), epoch);
            LinkedList<Integer> dv = new LinkedList<Integer>();
            dv.addAll(dvec);
            return dv;
        }
    }

    public static Map<Integer, Integer> getCurrentTimeVectorAsMap() {
        synchronized (dvec) {
            int epoch = Epoch.getEpochUnderExecution();
            dvec.set(ID.getClusterID(), epoch);
            Map<Integer, Integer> dv = new LinkedHashMap<>();
            for (int key = 0; key < dvec.size(); key++)
                dv.put(key, dvec.get(key));
            return dv;
        }
    }

    public static void updateLocalClock(Map<Integer, Integer> remoteClusterDepVectorMap) {
        //TODO Update local clock with the values of the remote vector data
        for (Map.Entry<Integer, Integer> t : remoteClusterDepVectorMap.entrySet()) {
            setValue(t.getKey(), t.getValue());
        }
    }
}