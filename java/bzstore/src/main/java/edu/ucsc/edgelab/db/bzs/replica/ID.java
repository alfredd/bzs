package edu.ucsc.edgelab.db.bzs.replica;

public class ID {
    private static Integer clusterID;
    private static Integer replicaID;

    private static boolean runBenchMarkTests = false;

    public static boolean canRunBenchMarkTests() {
        return runBenchMarkTests;
    }

    public static void setRunBenchMarkTests(boolean runBenchMarkTests) {
        ID.runBenchMarkTests = runBenchMarkTests;
    }

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

    public static String string() {
        return String.format("Cluster:Replica = %d:%d", ID.clusterID, ID.replicaID);
    }
}
