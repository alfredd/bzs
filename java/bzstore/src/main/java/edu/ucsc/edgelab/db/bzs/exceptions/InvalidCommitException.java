package edu.ucsc.edgelab.db.bzs.exceptions;

import org.rocksdb.RocksDBException;

public class InvalidCommitException extends Exception {

    public InvalidCommitException(String message) {
        super(message);
    }

    public InvalidCommitException(String message, RocksDBException e) {
        super(message,e);
    }
}
