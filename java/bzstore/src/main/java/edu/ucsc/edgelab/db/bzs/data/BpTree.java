package edu.ucsc.edgelab.db.bzs.data;

import edu.ucsc.edgelab.db.bzs.exceptions.InvalidCommitException;

import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

/**
 * Implementation Pending.
 */
class BpTree extends TreeMap<String, List<BZStoreData>> {


    public void commit(String key, String value, String digest) throws InvalidCommitException {
        /*
            not sure if this is the best way to do this.
         */
        if (digest==null) {
            throw new InvalidCommitException("Message digest cannot be null. Cannot commit.");
        } else if(value == null) {
            throw new InvalidCommitException("Value cannot be null. Cannot commit.");
        } else if(key==null) {
            throw new InvalidCommitException("Key cannot be null. Cannot commit.");
        }
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
                newVersion.digest = digest;
                this.get(key).add(0, newVersion);
            }
        }
    }
}
