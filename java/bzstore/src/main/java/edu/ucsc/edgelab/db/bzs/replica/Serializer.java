package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.data.BpTree;
import edu.ucsc.edgelab.db.bzs.Bzs;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class Serializer {

    private List<Bzs.Transaction> epochList = new LinkedList<>();
    // Keeping track of current object changes to the epochList.
    private HashMap<String, Integer> readMap = new HashMap<>();

    public void resetEpoch() {
        epochList.clear();
        readMap.clear();
    }

    public boolean readConflicts(Bzs.ReadHistory c, BpTree datastore){
        if(!readMap.containsKey(c.key)) {
            readMap.add(datastore.get(key));
        }
        // Handling case 2 and 3 from the table in the doc
        if(readMap.get(c.key) > c.version){
            return false;
        }
        return true;
    }

    public boolean serialize(Bzs.Transaction t, BpTree datastore) {
        for(Bzs.ReadHistory object : t.readHistory){
            if(readConflicts(object,datastore))
                return false;
        }
        // Handling case 2 from the table in the doc
        for(Bzs.writeOperations object : t.writeHistory){
            int version = readMap.get(object.key);
            readMap.put(object.key, version + 1);
        }
        epochList.add(t);
        return true;
    }

    public List<Bzs.Transaction> getEpochList() {
        return  epochList;
    }
}
