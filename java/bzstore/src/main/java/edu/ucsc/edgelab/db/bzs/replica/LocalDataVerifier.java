package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;

public class LocalDataVerifier {

    private final Integer clusterID;

    public LocalDataVerifier(Integer clusterID) {
        this.clusterID = clusterID;
    }

    public boolean containsLocalWrite(Bzs.Transaction transaction) {
        boolean containsLocalWrite =true;
        for (Bzs.Write writeOps : transaction.getWriteOperationsList()) {
            String writeKey = writeOps.getKey();
            containsLocalWrite = BZDatabaseController.containsKey(writeKey);
            if (containsLocalWrite)
                break;
        }
        return containsLocalWrite;
    }
}
