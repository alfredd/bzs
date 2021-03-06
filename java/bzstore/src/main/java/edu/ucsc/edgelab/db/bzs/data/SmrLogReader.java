package edu.ucsc.edgelab.db.bzs.data;

import edu.ucsc.edgelab.db.bzs.Bzs;
import org.rocksdb.RocksDBException;

public class SmrLogReader {

    public static void main(String[] args) throws RocksDBException {
        if (args.length != 2)
            System.exit(1);
        Integer cid = Integer.decode(args[0]);
        Integer rid = Integer.decode(args[1]);
        BackendDb db = new BackendDb(cid, rid);
        Integer epoch = db.getEpochNumber();
        for (int i = 0; i <= epoch; i++) {
            String epochNumber = Integer.toString(i);
            Bzs.SmrLogEntry smrlogEntry = db.getSmrBlock("S."+epochNumber);
            if (smrlogEntry != null)
                System.out.println(String.format("Epoch: %d, SMR LOG[%d]: %s", i, i, smrlogEntry.toString()));
            else
                System.out.println(String.format("Error. SMRLOG[%d] is null or not present in the log.", i));
        }
        db.close();
    }
}

