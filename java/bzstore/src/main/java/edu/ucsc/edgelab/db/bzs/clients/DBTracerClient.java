package edu.ucsc.edgelab.db.bzs.clients;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.DBTracerGrpc;
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

    public void shutdown() {
        log.log(Level.FINE, "Shutting down db tracer client instance.");
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Bzs.SmrLogEntry getSmrLogEntry(int epochNumber) {
        Bzs.EpochNumber request = Bzs.EpochNumber.newBuilder().setNumber(epochNumber).build();
        return blockingStub.getLogEntry(request);
    }

    public Bzs.SmrLogEntry getLatestSmrLogEntry() {
        Bzs.EpochNumber request = Bzs.EpochNumber.newBuilder().setNumber(-1).build();
        return blockingStub.getLatestLogEntry(request);
    }

    public void printSMRDetails(Bzs.SmrLogEntry logEntry) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Epoch Number: = "+ logEntry.getEpochNumber());
        stringBuilder.append("\n");
        stringBuilder.append("Total LRWT= "+ logEntry.getLRWTxnsCount());
        stringBuilder.append("\n");
        stringBuilder.append("Total Prepared DRWT= "+ logEntry.getPreparedDRWTxnsCount());
        stringBuilder.append("\n");
        stringBuilder.append("Total Committed DRWT= "+ logEntry.getCommittedDRWTxnsCount());
        stringBuilder.append("\n");
        stringBuilder.append("Total 2PC Prepared DRWT= "+ logEntry.getRemotePreparedDRWTsCount());
        stringBuilder.append("\n");
        stringBuilder.append("Total 2PC Committed DRWT= "+ logEntry.getRemoteCommittedDRWTsCount());
        stringBuilder.append("\n");
        stringBuilder.append("Dependency Vector= "+ logEntry.getDepVectorMap());
        stringBuilder.append("\n");
        stringBuilder.append("LCE = "+ logEntry.getLce());
        stringBuilder.append("\n");

        System.out.println(stringBuilder.toString());
    }

    public static void main(String[] args) {
        if (args.length !=2 ) {
            System.out.println("Input parameters must include space separated clusterid replicaid");
            System.exit(1);
        }
        int clusterID = 0, replicaID = 0;
        try {
            clusterID = Integer.valueOf(args[0]);
            replicaID = Integer.valueOf(args[1]);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        Scanner scanner = new Scanner(System.in);
        int epochNumber = 0;
        ServerInfo serverInfo = null;
        try {
            serverInfo = Configuration.getServerInfo(clusterID, replicaID);
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
                System.out.println("You chose to print SMR log for epoch number "+choice);
                epochNumber = Integer.valueOf(choice);
                Bzs.SmrLogEntry logEntry = dbTracerClient.getSmrLogEntry(epochNumber);
                System.out.println(logEntry);
            } else if (choice.equalsIgnoreCase("q")) {
                System.out.println("Bye!");
                scanner.close();
                dbTracerClient.shutdown();
                System.exit(0);
            } else if (choice.equalsIgnoreCase("n")) {
                epochNumber+=1;
                System.out.println("You chose next epoch: "+ epochNumber);
                Bzs.SmrLogEntry logEntry = dbTracerClient.getSmrLogEntry(epochNumber);
                epochNumber = logEntry.getEpochNumber();
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
