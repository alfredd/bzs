package edu.ucsc.edgelab.db.bzs.clientlib;

import edu.ucsc.edgelab.db.bzs.Bzs;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestROTransaction {

    @Test
    public void testROTxn() {
        Transaction rot = new Transaction();

        Map<Integer, Bzs.ROTransactionResponse> receivedResponses = new HashMap<>();

        receivedResponses.put(0,
                Bzs.ROTransactionResponse.newBuilder()
                        .addReadResponses(
                                Bzs.ReadResponse.newBuilder()
                                        .setReadOperation(Bzs.Read.newBuilder().setKey("111").setClusterID(0).build())
                                        .setValue("v1")
                                        .putDepVector(0, 10)
                                        .putDepVector(1, 7)
                                        .putDepVector(2, 8)
                                        .setLce(4)
                                        .build())
                        .build());
        receivedResponses.put(1,
                Bzs.ROTransactionResponse.newBuilder()
                        .addReadResponses(
                                Bzs.ReadResponse.newBuilder()
                                        .setReadOperation(Bzs.Read.newBuilder().setKey("22").setClusterID(0).build())
                                        .setValue("v2")
                                        .putDepVector(0, 7)
                                        .putDepVector(1, 9)
                                        .putDepVector(2, 4)
                                        .setLce(5)
                                        .build())
                        .build());
        receivedResponses.put(2,
                Bzs.ROTransactionResponse.newBuilder()
                        .addReadResponses(
                                Bzs.ReadResponse.newBuilder()
                                        .setReadOperation(Bzs.Read.newBuilder().setKey("3").setClusterID(0).build())
                                        .setValue("v3")
                                        .putDepVector(0, 8)
                                        .putDepVector(1, 6)
                                        .putDepVector(2, 10)
                                        .setLce(7)
                                        .build())
                        .build());
        List<Bzs.ReadResponse> secondROT = rot.validateAndGenerateSecondROTxns(receivedResponses);
        System.out.println("Transaction count for second ROT round: "+secondROT.size());
        System.out.println(secondROT);
    }
}
