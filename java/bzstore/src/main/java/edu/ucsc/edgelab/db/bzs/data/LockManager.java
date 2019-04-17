package edu.ucsc.edgelab.db.bzs.data;

import edu.ucsc.edgelab.db.bzs.Bzs;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private static final Map<String,Long> LOCKED = new ConcurrentHashMap<>();
    private static final Map<String, List<Bzs.Transaction>> LOCK_LIST = new ConcurrentHashMap<>();

    private static boolean isLocked(String key) {
        return LOCKED.containsKey(key);
    }

    private static boolean unlock(String key) {
        if (isLocked(key)) {
            LOCKED.remove(key);
        }
        return isLocked(key);
    }

    private static boolean lock(String key) {
        if (!isLocked(key)) {
            LOCKED.put(key,System.currentTimeMillis());
        }
        return isLocked(key);
    }

    public boolean checkLocks(Bzs.Transaction t) {
        boolean locked = true;
        for (int i =0 ;i<t.getWriteOperationsCount();i++) {

        }
        return locked;
    }

    public boolean addToLockQueue(Bzs.Transaction t) {
        boolean canLock=true;

        return canLock;
    }
}
