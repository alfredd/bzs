package edu.ucsc.edgelab;

import edu.ucsc.edgelab.db.bzs.bftcommit.BFTServer;

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
        //new BFTServer(Integer.parseInt(args[0]), new BpTree());
        new BFTServer( 3);
    }
}
