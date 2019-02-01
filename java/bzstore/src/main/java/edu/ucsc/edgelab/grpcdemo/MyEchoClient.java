package edu.ucsc.edgelab.grpcdemo;

import edu.ucsc.edgelab.db.bzs.EchoGrpc;
import edu.ucsc.edgelab.db.bzs.EchoMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class MyEchoClient {
    private static final Logger logger = Logger.getLogger(MyEchoClient.class.getName());

    private final ManagedChannel channel;
    private final EchoGrpc.EchoBlockingStub blockingStub;

    public MyEchoClient (String host, int port) {
        this(ManagedChannelBuilder.forAddress(host,port).usePlaintext().build());
    }

    MyEchoClient (ManagedChannel channel) {
        this.channel = channel;
        blockingStub = EchoGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void sendMessage( String message) {
        logger.info("Sending message to server...");
        EchoMessage request = EchoMessage.newBuilder().setMessage(message).build();
        EchoMessage response;
        response = blockingStub.echoThis(request);
        logger.info("Response message from server: ("+response.getMessage()+")");
    }

    public static void main(String[] args) throws InterruptedException {
        MyEchoClient client = new MyEchoClient("localhost", MyEchoServer.PORT);
        Scanner scanner = new Scanner(System.in);
        try {
            while (scanner.hasNext()) {
                String message = scanner.nextLine();
                client.sendMessage(message);
            }
        } finally {
            client.shutdown();
            scanner.close();
        }
    }
}
