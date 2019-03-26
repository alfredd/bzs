package edu.ucsc.edgelab.db.bzs.bftcommit;

import java.nio.ByteBuffer;

public class BftBPClient extends BFTClient {
    public BftBPClient(int ClientId) {
        super(ClientId);
    }

    byte[] generateByteArray(int n) {
        byte[] src = new byte[n];
        for (int i = 0; i < n; i++) {
            src[i]= (byte) (i%255);
        }
        return ByteBuffer.allocate(n).put(src).array();
    }

    public static void main(String[] args) {
        BftBPClient client = new BftBPClient(0);
        for (int i = 0; i < 10; i++) {
            client.sendBytesToBFTServer(client.generateByteArray(1024));
        }

    }

}
