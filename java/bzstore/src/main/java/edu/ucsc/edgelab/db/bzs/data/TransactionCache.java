package edu.ucsc.edgelab.db.bzs.data;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.replica.TransactionID;
import io.grpc.stub.StreamObserver;

import java.util.TreeMap;

public class TransactionCache {
    private static final TransactionCache CACHE = new TransactionCache();
    private static class TxnTuple {
        private Bzs.Transaction t;
        private StreamObserver<Bzs.TransactionResponse> o;
    }
    private TreeMap<TransactionID, TxnTuple> storage = new TreeMap<>();


    private TransactionCache() {}

    public static void add (TransactionID tid, Bzs.Transaction t, StreamObserver<Bzs.TransactionResponse> o) {
        synchronized (CACHE) {
            final TxnTuple tuple = new TxnTuple();
            tuple.t=t;
            tuple.o = o;
            CACHE.storage.put(tid, tuple);
        }
    }

    public static void update (TransactionID tid, Bzs.Transaction t) {
        synchronized (CACHE) {
            TxnTuple txnTuple = CACHE.storage.get(tid);
            txnTuple.t = t;
        }
    }

    public static Bzs.Transaction getTransaction(TransactionID tid) {
        final TxnTuple txnTuple = CACHE.storage.get(tid);
        return txnTuple==null? null: txnTuple.t;
    }

    public static StreamObserver<Bzs.TransactionResponse> getObserver(TransactionID tid) {
        final TxnTuple txnTuple = CACHE.storage.get(tid);
        return txnTuple==null? null: txnTuple.o;
    }

    public static void removeHistory(TransactionID tid) {
        synchronized (CACHE) {
            CACHE.storage.remove(tid);
        }
    }



}
