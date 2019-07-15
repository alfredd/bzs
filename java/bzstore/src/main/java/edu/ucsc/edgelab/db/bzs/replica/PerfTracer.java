package edu.ucsc.edgelab.db.bzs.replica;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PerfTracer {


    public static final Logger log = Logger.getLogger(PerformanceTrace.class.getName());
    private int metricCount;
    private Map<Integer, Metrics> batchMetrics = new TreeMap<>();
    private Map<Integer, Set<TransactionID>> tidMap = new TreeMap<>();
    private Map<TransactionID, TransactionMetrics> transactionMetrics = new TreeMap<>();
    private ReportBuilder reportBuilder;

    private static PerfTracer perfTracer = null;

    private PerfTracer() {
        String[] fields = new String[]{"Epoch Number, ",    // 1 Done
                "Total Txns in Epoch, ",                    // 2 Done
                "Total Local Txns in Epoch, ",              // 3 Done
                "Total Remote Txns in Epoch, ",             // 4 Done
                "Local Txns Prepared Count, ",              // 5 Done
                "Distributed Txns Prepared Count, ",        // 6 Done
                "Local Txns Committed In Epoch, ",          // 7 Done
                "Distributed Txns CommittedIn Epoch, ",     // 8 Done
                "Local Txns Failed In Epoch(F), ",          // 9 Done
                "Distributed Txns Failed In Epoch(F), ",    // 10 Done
                "Total Txns Committed, ",                   // 11 Sum (7, 8)
                "Total Txns Failed, ",                      // 12 Sum (9,10)
                "Local Txns Prepare Time(ms), ",            // 13 Done
                "Distributed Txns Prepare Time(ms), ",      // 14 Done
                "Local Txn Commit Time(ms), ",              // 15 Done
                "Dist Txn Commit Time(ms), ",               // 16 NA
                "Batch Completion Time(ms), ",              // 17 StopTime - StartTime
                "Txn Throughput(Tps), ",                    // 18 Done
                "Prepared Txn throughput (Tps), ",          // 19 Done
                "Prepared Txn throughput (Bps), ",          // 20 Done
                "Throughput (Bps), ",                       // 21 Done
                "Distributed Txn Avg Prepare Time(s), ",    // 22 Done
                "Distributed Txn Avg Commit Time(s), ",     // 23 Done
                "Distributed Txn Avg Completion Time(s), ", // 24 Done
                "Distributed Txn Avg Epoch Span, \n ",      // 25 Done
        };
        metricCount = 0;
        try {
            reportBuilder = new ReportBuilder("WedgeDB_perf_report", fields);
        } catch (IOException e) {
            log.log(Level.WARNING, "Exception occurred while creating the report builder. " + e.getLocalizedMessage(), e);
        }
    }

    public static void init() {
        if (perfTracer == null)
            perfTracer = new PerfTracer();
    }


    public static void updateLRWTCount(int lrwt) {

    }
    public static void updateDRWTCount() {

    }

    public static void updateTransactionCount () {

    }
}
