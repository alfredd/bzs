package edu.ucsc.edgelab.db.bzs.replica;


import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;
import edu.ucsc.edgelab.db.bzs.data.BZStoreData;
import edu.ucsc.edgelab.db.bzs.exceptions.InvalidDataAccessException;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

public class Serializer {

    public static final Logger LOGGER = Logger.getLogger(Serializer.class.getName());

    private List<Bzs.Transaction> epochList = new LinkedList<>();
    // Keeping track of current object changes to the epochList.
    private HashMap<String, Long> readMap = new HashMap<>();

    public void resetEpoch() {
        epochList.clear();
        readMap.clear();
    }

    public boolean readConflicts(Bzs.ReadHistory c) {
        //Needs to be changes where the version is fetched from the datastore and not the first key.
        BZStoreData data;
        try {
            data = BZDatabaseController.getlatest(c.getKey());
        } catch (InvalidDataAccessException e) {
            return true;
        }
        if (!readMap.containsKey(c.getKey())) {
            readMap.put(c.getKey(), Long.valueOf(data.version));
        }
        // Handling case 2 and 3 from the table in the google doc
        if (readMap.get(c.getKey()) > c.getVersion()) {
            return true;
        }
        return false;
    }

    public boolean serialize(Bzs.Transaction t) {
        for (Bzs.ReadHistory readHistory : t.getReadHistoryList()) {
            if (readConflicts(readHistory))
                return false;
        }
        LOGGER.info("Read map: "+readMap.toString());
        // Handling case 2 from the table in the google doc
        for (Bzs.Write writeOperation : t.getWriteOperationsList()) {
            String key = writeOperation.getKey();
            if (!readMap.containsKey(key)) {
                try {
                    BZStoreData data = BZDatabaseController.getlatest(key);
                    readMap.put(key,data.version);
                } catch (InvalidDataAccessException e) {
                    readMap.put(key, (long) 0);
                }
            }
            long version = readMap.get(key);
            readMap.put(key, version + 1);
        }
        epochList.add(t);
        return true;
    }

    public List<Bzs.Transaction> getEpochList() {
        return epochList;
    }
}
