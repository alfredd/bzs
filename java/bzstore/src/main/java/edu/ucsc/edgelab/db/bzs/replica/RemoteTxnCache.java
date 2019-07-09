package edu.ucsc.edgelab.db.bzs.replica;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.logging.Logger;

public class RemoteTxnCache {

    private RemoteTxnCache() {
    }

    public static final Logger logger = Logger.getLogger(RemoteTxnCache.class.getName());

    private static Set<String> batchIDs = new ConcurrentSkipListSet<>();

    private static Map<String, Set<TransactionID>> preparedTIDsForBatch = new ConcurrentHashMap<>();

    public static void addBatchIDToPrepared(String id) {
        batchIDs.add(id);
    }

    public static boolean isPrepared(String id) {
        boolean found = batchIDs.contains(id);
        logger.info(String.format("ID: %s found: ", id)+found +". Prepared batch IDs: "+ batchIDs);
        return found;
    }

    public static boolean isTIDInPreparedBatch(String id, TransactionID tid) {
        if (! isPrepared(id))
            return false;
        return preparedTIDsForBatch.get(id).contains(tid);
    }

    public static void addTIDsToPreparedBatch(String id, Set<TransactionID> preparedTIDs) {
        if (!isPrepared(id)) {
            addBatchIDToPrepared(id);
            preparedTIDsForBatch.put(id, preparedTIDs);
        } else {
            preparedTIDsForBatch.get(id).addAll(preparedTIDs);
        }
    }


}
