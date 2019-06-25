package edu.ucsc.edgelab.db.bzs.txn;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class WedgeDBThreadPoolExecutor {
    private final ThreadPoolExecutor concurrentExecutor;
    private ThreadPoolExecutor epochExecutor;

    public WedgeDBThreadPoolExecutor() {
        epochExecutor = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        this.concurrentExecutor = new ThreadPoolExecutor(4, 8, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
    }

    public void addToFixedQueue(EpochProcessor processor) {
        epochExecutor.execute(processor);
    }

    public void addToConcurrentQueue(Runnable txnProcessor) {
        concurrentExecutor.execute(txnProcessor);
    }
}
