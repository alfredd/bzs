package edu.ucsc.edgelab.db.bzs.data;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private static final Map<String,Long> LOCKED = new ConcurrentHashMap<>();

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
}
