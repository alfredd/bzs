package edu.ucsc.edgelab.db.bzs.txn;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class EpochThreadPoolExecutor {
    private ThreadPoolExecutor epochExecutor;

    public EpochThreadPoolExecutor() {
        epochExecutor = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
    }

    public void addToThreadPool(EpochProcessor processor) {
        epochExecutor.execute(processor);
    }
}
