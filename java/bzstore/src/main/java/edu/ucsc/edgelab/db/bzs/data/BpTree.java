package edu.ucsc.edgelab.db.bzs.data;

import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

/**
 * Implementation Pending.
 */
public class BpTree extends TreeMap<String, List<BZStoreData>> {


    public void commit(String key, BZStoreData data) {
        /*
            not sure if this is the best way to do this.
         */
        synchronized (this) {
            List<BZStoreData> list = this.get(key);
            if (list==null) {
                list = new LinkedList<>();
                this.put(key,list);
            }
            list.add(0,data);
        }
    }
}
