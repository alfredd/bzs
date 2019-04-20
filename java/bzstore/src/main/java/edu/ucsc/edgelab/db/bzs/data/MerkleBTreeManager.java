package edu.ucsc.edgelab.db.bzs.data;

import merklebtree.TreeNode;

import java.util.LinkedList;

public class MerkleBTreeManager {

    private LinkedList<TreeNode> merkleTreeSnapshots;

    public MerkleBTreeManager() {
        merkleTreeSnapshots = new LinkedList<>();
    }

    public void append(TreeNode root) {
        merkleTreeSnapshots.add(root);
    }

    public TreeNode getLatest() {
        return merkleTreeSnapshots.peekLast();
    }
}
