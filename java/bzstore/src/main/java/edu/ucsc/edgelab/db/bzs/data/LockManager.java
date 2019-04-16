package edu.ucsc.edgelab.db.bzs.data;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private static final Map<String,Long> LOCKED = new ConcurrentHashMap<>();
}
