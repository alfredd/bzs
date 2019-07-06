package edu.ucsc.edgelab.db.bzs.replica;


import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;
import edu.ucsc.edgelab.db.bzs.data.BZStoreData;
import edu.ucsc.edgelab.db.bzs.data.LockManager;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Serializer {

    public static final Logger LOGGER = Logger.getLogger(Serializer.class.getName());
    private boolean checkLocks = true;

    private List<Bzs.Transaction> epochList = new LinkedList<>();
    // Keeping track of current object changes to the epochList.
    private HashMap<String, Long> readMap = new HashMap<>();

    private Integer clusterID;
    private Integer replicaID;

    public static final Logger log = Logger.getLogger(Serializer.class.getName());


    public Serializer(Integer clusterID, Integer replicaID) {
        this.clusterID = clusterID;
        this.replicaID = replicaID;
        try {
            BZStoreProperties properties = new BZStoreProperties();
            Integer leader = Integer.decode(properties.getProperty(clusterID, BZStoreProperties.Configuration.leader));
            checkLocks= leader==replicaID;
        } catch (Exception e) {
            log.log(Level.WARNING, "Exception occurred while getting the leader replica id: "+e.getLocalizedMessage(), e);
            checkLocks = replicaID==0;
        }
    }

    @Deprecated
    public Serializer(Integer clusterID) {
        this.clusterID = clusterID;
    }

    public void resetEpoch() {
        epochList.clear();
        readMap.clear();
    }

    public void setClusterID(Integer clusterID) {
        this.clusterID = clusterID;
    }

    public void setReplicaID(Integer replicaID) {
        this.replicaID = replicaID;
    }

    public boolean readConflicts(Bzs.ReadHistory c) {
        //Needs to be changes where the version is fetched from the datastore and not the first key.
        BZStoreData data;
        Bzs.Read readOperation = c.getReadOperation();
        data = BZDatabaseController.getlatest(readOperation.getKey());
        if (data.version < 0) {
            return true;
        }
        if (!readMap.containsKey(readOperation.getKey())) {
            readMap.put(readOperation.getKey(), Long.valueOf(data.version));
        }
        // Handling case 2 and 3 from the table in the google doc
        if (readMap.get(readOperation.getKey()) > c.getVersion() && checkLocks && LockManager.isLocked(readOperation.getKey())) {
            return true;
        }
        return false;
    }

    public boolean serialize(Bzs.Transaction t) {

        for (Bzs.ReadHistory readHistory : t.getReadHistoryList()) {
            if (readHistory.getReadOperation().getClusterID() == this.clusterID && readConflicts(readHistory))
                return false;
        }
        // Handling case 2 from the table in the google doc
        for (Bzs.Write writeOperation : t.getWriteOperationsList()) {
            String key = writeOperation.getKey();
            if (!readMap.containsKey(key)) {
                BZStoreData data = BZDatabaseController.getlatest(key);
                readMap.put(key, data.version);
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
