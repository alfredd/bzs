package edu.ucsc.edgelab.db.bzs.data;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;

import java.util.TreeMap;

public class TransactionCache {
    private static final TransactionCache CACHE = new TransactionCache();
    private TreeMap<TransactionID, Bzs.Transaction> storage = new TreeMap<>();


    private TransactionCache() {}

    public static void update (TransactionID tid, Bzs.Transaction t) {
        synchronized (CACHE) {
            CACHE.storage.put(tid,t);
        }
    }

    public static Bzs.Transaction get(TransactionID tid) {
//        synchronized (CACHE) {
            return CACHE.storage.get(tid);
//        }
    }

    public static Bzs.Transaction remove(TransactionID tid) {
        synchronized (CACHE) {
            return CACHE.storage.remove(tid);
        }
    }

}
