package edu.ucsc.edgelab.db.bzs.replica;

public class ID {
    private static Integer clusterID;
    private static Integer replicaID;

    public static Integer getClusterID() {
        return clusterID;
    }

    public static Integer getReplicaID() {
        return replicaID;
    }

    public static void setIDs(Integer clusterID, Integer replicaID) {
        ID.clusterID = clusterID;
        ID.replicaID = replicaID;
    }
}
