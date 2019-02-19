package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.data.BpTree;
import edu.ucsc.edgelab.db.bzs.Bzs;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class Serializer {

    private List<Bzs.Transaction> epochList = new LinkedList<>();
    // Keeping track of current object changes to the epochList.
    private HashMap<String, Long> readMap = new HashMap<>();

    public void resetEpoch() {
        epochList.clear();
        readMap.clear();
    }

    public boolean readConflicts(Bzs.ReadHistory c, BpTree datastore){
        //Needs to be changes where the version is fetched from the datastore and not the first key.
        if(!readMap.containsKey(c.getKey())){
            readMap.put(c.getKey(), Long.valueOf(datastore.get(c.getKey()).get(0).version));
        }
        // Handling case 2 and 3 from the table in the google doc
        if(readMap.get(c.getKey()) > c.getVersion()){
            return false;
        }
        return true;
    }

    public boolean serialize(Bzs.Transaction t, BpTree datastore) {
        for(Bzs.ReadHistory object : t.getReadHistoryList()){
            if(readConflicts(object,datastore))
                return false;
        }
        // Handling case 2 from the table in the google doc
        for(Bzs.Write object : t.getWriteOperationsList()){
            long version = readMap.get(object.getKey());
            readMap.put(object.getKey(), version + 1);
        }
        epochList.add(t);
        return true;
    }

    public List<Bzs.Transaction> getEpochList() {
        return  epochList;
    }
}
