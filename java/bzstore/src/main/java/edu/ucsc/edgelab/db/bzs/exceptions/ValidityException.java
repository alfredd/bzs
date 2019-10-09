package edu.ucsc.edgelab.db.bzs.exceptions;

import java.util.List;

public class ValidityException extends Exception {
    public ValidityException() {
    }

    public ValidityException(String message) {
        super(message);
    }
    public ValidityException(String message, List<Integer> checkedClusterProofs) {
        super(message);
        setCheckedClusterProofs(checkedClusterProofs);
    }

    public ValidityException(String message, Throwable cause) {
        super(message, cause);
    }

    private List<Integer> checkedClusterProofs;

    public List<Integer> getCheckedClusterProofs() {
        return checkedClusterProofs;
    }

    public void setCheckedClusterProofs(List<Integer> checkedClusterProofs) {
        this.checkedClusterProofs = checkedClusterProofs;
    }
}
