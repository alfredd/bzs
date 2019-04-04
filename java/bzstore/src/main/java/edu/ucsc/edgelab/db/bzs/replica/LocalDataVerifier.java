package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;

public class LocalDataVerifier {

    private final Integer clusterID;

    public LocalDataVerifier(Integer clusterID) {
        this.clusterID = clusterID;
    }

    public boolean containsLocalCommit(Bzs.Transaction transaction) {
        boolean containsLocal =true;


        return containsLocal;
    }
}
