package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;

import java.util.LinkedHashMap;
import java.util.Map;

public class Timestamp implements Comparable {
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

    public Bzs.VectorClock getVectorTime() {
        Bzs.VectorClock.Builder clockBuilder = Bzs.VectorClock.newBuilder();

        for (Map.Entry<Integer, Integer> entry : timestamp.entrySet()) {
            clockBuilder.addTimestamps(Bzs.Timestamp.newBuilder().setClusterID(entry.getKey()).setEpochNumber(entry.getValue()).build());
        }
        return clockBuilder.build();
    }

/*    public static void main(String[] args) {
        Timestamp t = new Timestamp();
        t.add(0, 4);
        t.add(1, 3);
        t.add(2, 5);
        Bzs.VectorClock clock = t.getVectorTime();
        for (int i = 0; i < clock.getTimestampsCount(); i++) {
            System.out.println(String.format("%d -> %d", clock.getTimestamps(i).getClusterID(),
                    clock.getTimestamps(i).getEpochNumber()));
        }
        System.out.println(clock.toString());
    }*/
}
