package edu.ucsc.edgelab.db.bzs.replica;

import org.junit.Test;

public class PerformanceTraceTest {
    @Test
    public void testTransactionCount() {
        PerformanceTrace trace = new PerformanceTrace();
        trace.setTotalTransactionCount(1,
                100, 20, 80);

    }
}
