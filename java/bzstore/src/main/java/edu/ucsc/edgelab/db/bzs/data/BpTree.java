package edu.ucsc.edgelab.db.bzs.data;

import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

/**
 * Implementation Pending.
 */
public class BpTree extends TreeMap<String, List<BZStoreData>> {


    public void commit(String key, String value) {
        /*
            not sure if this is the best way to do this.
         */
        synchronized (this) {

            BZStoreData newVersion = new BZStoreData();
            newVersion.value = value;
            newVersion.version = "0";
            if (!this.containsKey(key)) {
                List<BZStoreData> list = new LinkedList<>();
                list.add(newVersion);
                this.put(key, list);
            }
            else {
                newVersion.version = String.valueOf(Long.parseLong(this.get(key).get(0).version) + 1);
                this.get(key).add(0, newVersion);
            }
        }
    }
}
