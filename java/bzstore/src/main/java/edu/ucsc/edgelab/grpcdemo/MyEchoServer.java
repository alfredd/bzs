package edu.ucsc.edgelab.grpcdemo;

import edu.ucsc.edgelab.db.bzs.EchoGrpc;
import edu.ucsc.edgelab.db.bzs.EchoMessage;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.logging.Logger;

public class MyEchoServer {

    public static final int PORT = 45002;

    private Server server;
    private static final Logger logger = Logger.getLogger(MyEchoServer.class.getName());

    public static void main(String[] args) throws IOException, InterruptedException {
        MyEchoServer mes = new MyEchoServer();
        mes.start();
        mes.blockUntilShutdown();
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }


    private void start() throws IOException {
        int port = PORT;
        server = ServerBuilder.forPort(port).addService(new EchoImpl()).build().start();
        logger.info("Server started.");
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Shutting down.");
                MyEchoServer.this.stop();
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    static class EchoImpl extends EchoGrpc.EchoImplBase {
        @Override
        public void echoThis(EchoMessage request, StreamObserver<EchoMessage> responseObserver) {
            EchoMessage message = EchoMessage.newBuilder().setMessage("EchoServer: " + request.getMessage()).build();
            responseObserver.onNext(message);
            responseObserver.onCompleted();
        }
    }


}
