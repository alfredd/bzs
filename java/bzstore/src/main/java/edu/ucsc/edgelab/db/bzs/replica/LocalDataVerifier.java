package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;

public class LocalDataVerifier {

    private final Integer clusterID;

    public LocalDataVerifier(Integer clusterID) {
        this.clusterID = clusterID;
    }

    public boolean containsLocalWrite(Bzs.Transaction transaction) {
        boolean containsLocal =true;
        for (Bzs.Write writeOps : transaction.getWriteOperationsList()) {
            String writeKey = writeOps.getKey();
            writeKey.split(":");
        }

        return containsLocal;
    }
}
