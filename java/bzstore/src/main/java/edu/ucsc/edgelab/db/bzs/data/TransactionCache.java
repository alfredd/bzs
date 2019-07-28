package edu.ucsc.edgelab.db.bzs.data;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;
import io.grpc.stub.StreamObserver;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;

public class TransactionCache {
    private static final TransactionCache CACHE = new TransactionCache();
    private static Logger logger = Logger.getLogger(TransactionCache.class.getName());

    private static class TxnTuple {
        private Bzs.Transaction t;
        private StreamObserver<Bzs.TransactionResponse> o;
        private Set<String> writeOps = new HashSet<>();
        private Bzs.TransactionResponse.Builder rb = Bzs.TransactionResponse.newBuilder();
    }

    private TreeMap<TransactionID, TxnTuple> storage = new TreeMap<>();


    private TransactionCache() {
    }

    public static void add(TransactionID tid, Bzs.Transaction t, StreamObserver<Bzs.TransactionResponse> o) {
        synchronized (CACHE) {
            final TxnTuple tuple = new TxnTuple();
            tuple.t = t;
            tuple.o = o;
            for (int i = 0; i < t.getWriteOperationsCount(); i++) {
                tuple.writeOps.add(t.getWriteOperations(i).getKey());
            }
            CACHE.storage.put(tid, tuple);
        }
    }

    public static void update(TransactionID tid, Bzs.Transaction t) {
        synchronized (CACHE) {
            TxnTuple txnTuple = CACHE.storage.get(tid);
            txnTuple.t = t;
        }
    }

    public static Bzs.Transaction getTransaction(TransactionID tid) {
        final TxnTuple txnTuple = CACHE.storage.get(tid);
        return txnTuple == null ? null : txnTuple.t;
    }

    public static StreamObserver<Bzs.TransactionResponse> getObserver(TransactionID tid) {
        final TxnTuple txnTuple = CACHE.storage.get(tid);
        return txnTuple == null ? null : txnTuple.o;
    }

    public static void removeHistory(TransactionID tid) {
        synchronized (CACHE) {
            CACHE.storage.remove(tid);
        }
    }


    public static void updateResponse(TransactionID tid, String key, String value, long version, int clusterID) {
        synchronized (CACHE) {
            TxnTuple tuple = CACHE.storage.get(tid);
            if (tuple.writeOps.contains(key)) {
                Bzs.WriteResponse writeResponse = Bzs.WriteResponse.newBuilder()
                        .setWriteOperation(Bzs.Write.newBuilder().setKey(key).setValue(value).setClusterID(clusterID).build())
                        .setVersion(version).build();
                tuple.rb = tuple.rb.addWriteResponses(writeResponse);
//                tuple.writeOps.remove(key);
                logger.info(String.format("Updated transaction response entry for TID: %s, (%s, %s, %d). Updated response is: %s", tid.toString(),
                        key, value, version, tuple.rb.build().toString()));
            }
        }
    }

    public static void updateDepVecInfo(TransactionID tid, Map<Integer, Integer> depVec) {
        synchronized (CACHE) {
            TxnTuple tuple = CACHE.storage.get(tid);
            tuple.rb = tuple.rb.putAllDepVector(depVec);
        }
    }

    public static Bzs.TransactionResponse getResponse(TransactionID tid) {
        synchronized (CACHE) {
            return CACHE.storage.get(tid).rb.build();
        }
    }

}
