package edu.ucsc.edgelab.db.bzs.replica;

import edu.ucsc.edgelab.db.bzs.Bzs;
import edu.ucsc.edgelab.db.bzs.clientlib.Transaction;
import edu.ucsc.edgelab.db.bzs.data.BZDatabaseController;
import edu.ucsc.edgelab.db.bzs.data.BZStoreData;
import edu.ucsc.edgelab.db.bzs.exceptions.InvalidCommitException;
import org.junit.Ignore;

import static org.junit.Assert.assertTrue;

public class SerializerTest {
    @Ignore
    public void serialTest() {

//        BZDatabaseController.clearDatabase();
        try {
            String testKey = "mytestkey";
            String mytestvalue = "mytestvalue";

            //Creating simple
            BZStoreData obj1 = new BZStoreData();
            obj1.value = mytestvalue;
            obj1.version = 1;
//            obj1.digest = "hello";

            Bzs.ReadHistory h1 =
                    Bzs.ReadHistory.newBuilder().setReadOperation(Bzs.Read.newBuilder().setKey(testKey).build()).setVersion(obj1.version).setValue(obj1.value).build();
            Bzs.Write w1 = Bzs.Write.newBuilder().setKey(testKey).setValue("newval").build();
            Bzs.Transaction t1 = Bzs.Transaction.newBuilder().addReadHistory(h1).addWriteOperations(w1).build();

            Bzs.ReadHistory h2 = Bzs.ReadHistory.newBuilder().setReadOperation(Bzs.Read.newBuilder().setKey(testKey).build()).setVersion(1).build();
            Bzs.Transaction t2 = Bzs.Transaction.newBuilder().addReadHistory(h2).build();

            BZDatabaseController.commit(testKey, obj1);
            Serializer s1 = new Serializer(0);
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
            Bzs.ReadHistory h3 = Bzs.ReadHistory.newBuilder().setReadOperation(Bzs.Read.newBuilder().setKey(testKey).build()).setVersion(-1).build();
            Bzs.Transaction t3 = Bzs.Transaction.newBuilder().addReadHistory(h3).build();
            assertTrue(s1.serialize(t3) == false);

            s1.resetEpoch();
            Bzs.ReadHistory h4 = Bzs.ReadHistory.newBuilder().setReadOperation(Bzs.Read.newBuilder().setKey(testKey).build()).setVersion(0).build();
            Bzs.Transaction t4 = Bzs.Transaction.newBuilder().addReadHistory(h4).build();
            assertTrue(s1.serialize(t2) == true);
            assertTrue(s1.serialize(t4) == false);


//            assertTrue(s1.serialize(t1) == true);
//            assertTrue(s1.serialize(t2) == true);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @Ignore
    public void testSerialization_1() throws InvalidCommitException {
        Transaction t1 = new Transaction();
        t1.write("x", "10");
        t1.write("y", "1");
        t1.write("z", "3");
        Bzs.Transaction tr1 = t1.getTransaction();
        Serializer serializer = new Serializer(0);
        assertTrue(serializer.serialize(tr1));
        BZDatabaseController.commit("x", new BZStoreData("10", 1));
        BZDatabaseController.commit("y", new BZStoreData("1", 1));
        BZDatabaseController.commit("z", new BZStoreData("3", 1));


        Transaction t2 = new Transaction();
        t2.setReadHistory("x", "10", 1, 0);
        t2.write("x", "10" + 45);
        t2.write("z", "5");
        Bzs.Transaction tr2 = t2.getTransaction();

        assertTrue(serializer.serialize(tr2));
        BZDatabaseController.commit("x", new BZStoreData("1045", 2));
        BZDatabaseController.commit("z", new BZStoreData("5", 1));

    }

    @Ignore
    public void testSerialization_1_with_epochReset() throws InvalidCommitException {
//        BZDatabaseController.clearDatabase();
        Transaction t1 = new Transaction();
        t1.write("x", "10");
        t1.write("y", "1");
        t1.write("z", "3");
        Bzs.Transaction tr1 = t1.getTransaction();
        Serializer serializer = new Serializer(0);
        assertTrue(serializer.serialize(tr1));
        BZDatabaseController.commit("x", new BZStoreData("10"));
        BZDatabaseController.commit("y", new BZStoreData("1"));
        BZDatabaseController.commit("z", new BZStoreData("3"));

        serializer.resetEpoch();
        BZStoreData data = BZDatabaseController.getlatest("x");

        Transaction t2 = new Transaction();
        t2.setReadHistory("x", data.value, data.version, 0);
        t2.write("x", "10" + 45);
        t2.write("z", "5");
        Bzs.Transaction tr2 = t2.getTransaction();

        assertTrue(serializer.serialize(tr2));
        BZDatabaseController.commit("x", new BZStoreData("1045", 2));
        BZDatabaseController.commit("z", new BZStoreData("5", 1));

    }

}
