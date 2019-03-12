package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;
import edu.ucsc.edgelab.db.bzs.data.BZStoreData;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class SerializerTest {
    @Test
    public  void serialTest (){
        try {
            String testKey = "mytestkey";
            String mytestvalue = "mytestvalue";

            //Creating simple
            BZStoreData obj1 = new BZStoreData();
            obj1.value = mytestvalue;
            obj1.version = 0;
            obj1.digest = "hello";

            Bzs.ReadHistory h1 = Bzs.ReadHistory.newBuilder().setKey(testKey).setVersion(0).build();
            Bzs.Write w1 = Bzs.Write.newBuilder().setKey(testKey).setValue("newval").build();
            Bzs.Transaction t1 = Bzs.Transaction.newBuilder().addReadHistory(h1).addWriteOperations(w1).build();

            Bzs.ReadHistory h2 = Bzs.ReadHistory.newBuilder().setKey(testKey).setVersion(0).build();
            Bzs.Transaction t2 = Bzs.Transaction.newBuilder().addReadHistory(h2).build();

            BZDatabaseController.commit(testKey, obj1);
            Serializer s1 = new Serializer();
            //
            s1.resetEpoch();
            assertTrue(s1.serialize(t1) == true);
            //
            s1.resetEpoch();
            assertTrue(s1.serialize(t1) == true);
            assertTrue(s1.serialize(t2) == false);

            s1.resetEpoch();
            assertTrue(s1.serialize(t2) == true);
            assertTrue(s1.serialize(t1) == true);

            s1.resetEpoch();
            Bzs.ReadHistory h3 = Bzs.ReadHistory.newBuilder().setKey(testKey).setVersion(-1).build();
            Bzs.Transaction t3 = Bzs.Transaction.newBuilder().addReadHistory(h3).build();
            assertTrue(s1.serialize(t3) == false);

            s1.resetEpoch();
            Bzs.ReadHistory h4 = Bzs.ReadHistory.newBuilder().setKey(testKey).setVersion(0).build();
            Bzs.Transaction t4 = Bzs.Transaction.newBuilder().addReadHistory(h4).build();
            assertTrue(s1.serialize(t2) == true);
            assertTrue(s1.serialize(t4) == true);


//            assertTrue(s1.serialize(t1) == true);
//            assertTrue(s1.serialize(t2) == true);

        }
        catch (Exception e){
            System.out.println(e.getMessage());
        }
    }
}
