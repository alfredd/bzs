package edu.ucsc.edgelab.db.bzs.data;

import edu.ucsc.edgelab.db.bzs.Bzs;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private static final Map<String,Long> LOCKS = new ConcurrentHashMap<>();

    private static boolean isLocked(String key) {
        return LOCKS.containsKey(key);
    }

    private LockManager() {}

    private static boolean unlock(String key) {
        if (isLocked(key)) {
            LOCKS.remove(key);
        }
        return isLocked(key);
    }

    private static boolean lock(String key) {
        if (!isLocked(key)) {
            LOCKS.put(key,System.currentTimeMillis());
        }
        return isLocked(key);
    }

    public static void  releaseLocks(Bzs.Transaction t) {
        for (int i =0 ;i<t.getWriteOperationsCount();i++) {
            unlock(t.getWriteOperations(i).getKey());
        }
        for (int i =0; i<t.getReadHistoryCount();i++) {
            unlock(t.getReadHistory(i).getKey());
        }
    }


    public static void  acquireLocks(Bzs.Transaction t) {
        for (int i =0 ;i<t.getWriteOperationsCount();i++) {
            lock(t.getWriteOperations(i).getKey());
        }
        for (int i =0; i<t.getReadHistoryCount();i++) {
            lock(t.getReadHistory(i).getKey());
        }
    }


}
