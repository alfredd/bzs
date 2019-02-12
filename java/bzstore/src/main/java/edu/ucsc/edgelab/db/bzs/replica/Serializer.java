package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.BpTree;
import edu.ucsc.edgelab.db.bzs.Commit;

import java.util.LinkedList;
import java.util.List;

public class Serializer {

    private List<Commit.Transaction> epochList = new LinkedList<>();

    public void resetEpoch() {
        epochList.clear();
    }

    public boolean serialize(Commit.Transaction t, BpTree datastore) {
        boolean status = false;

        return status;
    }

    public List<Commit.Transaction> getEpochList() {
        return  epochList;
    }
}
