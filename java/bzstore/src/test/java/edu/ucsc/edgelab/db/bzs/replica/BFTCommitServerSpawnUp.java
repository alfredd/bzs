package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.bftcommit.BFTClient;
import edu.ucsc.edgelab.db.bzs.bftcommit.BFTServer;
import edu.ucsc.edgelab.db.bzs.data.BpTree;

public class BFTCommitServerSpawnUp {
    public static void main(String args[]){
//        int serverIds[] = {0, 1, 2, 3};
//
//        BFTServer[] servers =  new BFTServer[serverIds.length];
//
//        int i = 0;
//        while(i < serverIds.length){
//            try {
//                servers[i] = new BFTServer(serverIds[i], new BpTree());
//            }
//            catch (Exception e){
//                i++;
//            }
//        }


        new BFTServer(3, new BpTree());
    }
}
