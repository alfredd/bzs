package edu.ucsc.edgelab.db.bzs.bftcommit;

import edu.ucsc.edgelab.db.bzs.replica.ReportBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BftBPClient extends BFTClient {


    public BftBPClient(int ClientId) throws IOException {
        super(ClientId);

    }

    byte[] generateByteArray(int n) {
        byte[] src = new byte[n];
        for (int i = 0; i < n; i++) {
            src[i] = (byte) (i % 255);
        }
        return ByteBuffer.allocate(n).put(src).array();
    }

    public static void main(String[] args) throws IOException {

        BftBPClient client = new BftBPClient(0);
        ReportBuilder report = new ReportBuilder("Report_1KB_commit", new String[]{
                "Commit ID, ",
                "Total bytes, ",
                "Bytes in batch, ",
                "Bytes Committed, ",
                "Bytes Failed, ",
                "Throughput (Bps),",
                "Duration (ms)\n"
        });
        int id = 0;
        long startTime = 0;
        long endTime = 0;
        int bytes = 1024*1;
        int byteCount = 0;
        for (int i = 0; i < 10; i++) {
            byteCount += 1;
            startTime = System.currentTimeMillis();
            byte[] committed = client.sendBytesToBFTServer(client.generateByteArray(bytes));
            endTime = System.currentTimeMillis();

            int returnLen = ByteBuffer.wrap(committed).getInt();
            int failedCount = bytes - returnLen;
            int processedCount = returnLen;

            long latency = endTime - startTime;
            double throughput = latency == 0 ? 0 : (double) processedCount * 1000 / (latency);
            String reportLine = String.format("%d, %d, %d, %d, %d, %f, %d\n",
                    id,
                    bytes * byteCount,
                    bytes,
                    processedCount,
                    failedCount,
                    throughput,
                    latency
            );
            report.writeLine(reportLine);
            id += 1;

        }
        report.close();

    }

}
