package edu.ucsc.edgelab.db.bzs.data;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.replica.ID;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private static final Map<String, Long> LOCKS = new ConcurrentHashMap<>();

    public static boolean isLocked(String key) {
        return LOCKS.containsKey(key);
    }

    private LockManager() {
    }

    private static boolean unlock(String key) {
        if (isLocked(key)) {
            LOCKS.remove(key);
        }
        return isLocked(key);
    }

    private static boolean lock(String key) {
        if (!isLocked(key)) {
            LOCKS.put(key, System.currentTimeMillis());
        }
        return isLocked(key);
    }

    public static void releaseLocks(Bzs.Transaction t) {
        if (t != null) {

            for (int i = 0; i < t.getWriteOperationsCount(); i++) {
                unlock(t.getWriteOperations(i).getKey());
            }
            for (int i = 0; i < t.getReadHistoryCount(); i++) {
                unlock(t.getReadHistory(i).getReadOperation().getKey());
            }
        }
    }

    public static void releaseLocks(Bzs.TransactionResponse t) {
        if (t != null) {
            for (int i = 0; i < t.getWriteResponsesCount(); i++) {
                Bzs.WriteResponse wr = t.getWriteResponses(i);
                    unlock(wr.getWriteOperation().getKey());
            }
            for (int i = 0; i < t.getReadHistoryCount(); i++) {
                unlock(t.getReadHistory(i).getReadOperation().getKey());
            }
        }
    }


    public static void acquireLocks(Bzs.Transaction t) {
        if (t != null) {
            Integer cid = ID.getClusterID();
            for (int i = 0; i < t.getWriteOperationsCount(); i++) {
                Bzs.Write writeOp = t.getWriteOperations(i);
                if (writeOp.getClusterID() == cid)
                    lock(writeOp.getKey());
            }
/*            for (int i = 0; i < t.getReadHistoryCount(); i++) {
                Bzs.ReadHistory readHistory = t.getReadHistory(i);
                lock(readHistory.getKey());
            }*/
        }
    }


}
