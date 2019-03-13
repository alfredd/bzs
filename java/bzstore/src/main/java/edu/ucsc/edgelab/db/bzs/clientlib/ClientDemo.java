package edu.ucsc.edgelab.db.bzs.clientlib;

import edu.ucsc.edgelab.db.bzs.configuration.Configuration;
import edu.ucsc.edgelab.db.bzs.configuration.ServerInfo;

import java.io.IOException;

public class ClientDemo {

    public static void main(String[] args) throws IOException {
        ServerInfo leader = Configuration.getLeaderInfo();
        Transaction t1 = new Transaction(leader.host,leader.port);
        t1.write("x","10");
        t1.write("y","1");
        t1.write("z","3");
        t1.commit();
        Transaction t2 = new Transaction(leader.host,leader.port);
        String vx = t2.read("x");
        t2.write("x",vx+45);
        t2.write("z","5");
        t2.commit();



    }
}
