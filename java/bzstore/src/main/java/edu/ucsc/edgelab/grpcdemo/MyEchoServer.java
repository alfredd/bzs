package edu.ucsc.edgelab.grpcdemo;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.logging.Logger;

public class MyEchoServer {

    static final int PORT = 45002;

    private Server server;
    private static final Logger logger = Logger.getLogger(MyEchoServer.class.getName());

    public static void main(String[] args) throws IOException, InterruptedException {
        MyEchoServer mes = new MyEchoServer();
        mes.start();
        mes.blockUntilShutdown();
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            logger.info("Awaiting termination.");
            server.awaitTermination();
        }
    }


    private void start() throws IOException {
        server = ServerBuilder.forPort(PORT).addService(new EchoImpl()).build().start();
        logger.info("Server started.");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down.");
            MyEchoServer.this.stop();
        }));
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }


}
