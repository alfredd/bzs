package edu.ucsc.edgelab.db.bzs.exceptions;

public class InvalidCommitException extends Exception {

    public InvalidCommitException() {
    }

    public InvalidCommitException(String message) {
        super(message);
    }

    public InvalidCommitException(String message, Throwable cause) {
        super(message, cause);
    }
}
