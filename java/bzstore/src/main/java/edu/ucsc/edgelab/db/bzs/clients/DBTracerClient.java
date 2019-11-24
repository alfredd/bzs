package edu.ucsc.edgelab.db.bzs.clients;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.DBTracerGrpc;
import edu.ucsc.edgelab.db.bzs.configuration.BZStoreProperties;
import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import edu.ucsc.edgelab.db.bzs.configuration.ServerInfo;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DBTracerClient {

    private final DBTracerGrpc.DBTracerBlockingStub blockingStub;

    private static final Logger log = Logger.getLogger(DBTracerClient.class.getName());


    private String host;
    private int port;

    private final ManagedChannel channel;

    public DBTracerClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext().build());
        this.host = host;
        this.port = port;
    }

    private DBTracerClient(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = DBTracerGrpc.newBlockingStub(channel);
    }


    public boolean isConnected() {
        return !channel.isTerminated();
    }

    public void shutdown() throws InterruptedException {
        log.log(Level.FINE, "Shutting down forwarding client instance.");
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public Bzs.SmrLogEntry getSmrLogEntry(int epochNumber) {
        Bzs.EpochNumber request = Bzs.EpochNumber.newBuilder().setNumber(epochNumber).build();
        return blockingStub.getLogEntry(request);
    }

    public Bzs.SmrLogEntry getLatestSmrLogEntry() {
        Bzs.EpochNumber request = Bzs.EpochNumber.newBuilder().setNumber(-1).build();
        return blockingStub.getLatestLogEntry(request);
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        int epochNumber = 0;
        ServerInfo serverInfo = null;
        try {
            serverInfo = Configuration.getServerInfo(0, 0);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Could not load server configurations.");
            System.exit(1);
        }
        DBTracerClient dbTracerClient = new DBTracerClient(serverInfo.host, serverInfo.port);

        for (;;) {
            System.out.print("SMR LOG Tracer\nEnter your choices:\na number for epoch number\nq to exit\nn for next epoch\nl for latest epoch\n\nchoice? ");
            String choice = scanner.next();
            if (choice.matches("[\\d]+")) {
                System.out.println("You chose to pritn SMR log for epoch number "+choice);
                epochNumber = Integer.valueOf(choice);
                Bzs.SmrLogEntry logEntry = dbTracerClient.getSmrLogEntry(epochNumber);
                System.out.println(logEntry);
            } else if (choice.equalsIgnoreCase("q")) {
                System.out.println("Bye!");
                System.exit(0);
            } else if (choice.equalsIgnoreCase("n")) {
                epochNumber+=1;
                System.out.println("You chose next epoch: "+ epochNumber);
                Bzs.SmrLogEntry logEntry = dbTracerClient.getSmrLogEntry(epochNumber);
                System.out.println(logEntry);
            } else if (choice.equalsIgnoreCase("l")) {
                System.out.println("You chose latest epoch");
                Bzs.SmrLogEntry logEntry = dbTracerClient.getLatestSmrLogEntry();
                System.out.println(logEntry);
            } else {
                System.out.println("Unrecognized choice. Try again.");
            }
        }
    }
}
