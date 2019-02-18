package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.data.BpTree;
import edu.ucsc.edgelab.db.bzs.Bzs;

import java.util.LinkedList;
import java.util.List;

public class Serializer {

    private List<Bzs.Transaction> epochList = new LinkedList<>();

    public void resetEpoch() {
        epochList.clear();
    }

    public boolean serialize(Bzs.Transaction t, BpTree datastore) {
        boolean status = false;

        return status;
    }

    public List<Bzs.Transaction> getEpochList() {
        return  epochList;
    }
}
