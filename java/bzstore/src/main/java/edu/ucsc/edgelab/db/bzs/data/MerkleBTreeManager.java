package edu.ucsc.edgelab.db.bzs.data;

import com.google.protobuf.ByteString;
import edu.ucsc.edgelab.db.bzs.Bzs;
import merklebtree.MerkleBTree;
import merklebtree.TreeNode;

import java.io.IOException;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MerkleBTreeManager {

    private Logger log = Logger.getLogger(MerkleBTreeManager.class.getName());

    private LinkedList<TreeNode> merkleTreeSnapshots;
    private MerkleBTree merkleBTree;

    public MerkleBTreeManager() {
        merkleTreeSnapshots = new LinkedList<>();
        merkleBTree = new MerkleBTree();
    }

    public void append(TreeNode root) {
        merkleTreeSnapshots.add(root);
    }

    public TreeNode getLatest() {
        return merkleTreeSnapshots.peekLast();
    }


    public Bzs.MerkleTree insert(String key, Bzs.DBData data, boolean storeMBTreeSnapshot) {
        if (storeMBTreeSnapshot) {
            storeMerkleTreeSnapShot();
        }
        try {
            byte[] rootHash = merkleBTree.put(key.getBytes(), data.toByteArray());

            Bzs.MerkleTree tree = Bzs.MerkleTree.newBuilder()
                    .setRoot(ByteString.copyFrom(rootHash))
                    .build();
            return tree;
        } catch (IOException e) {
            log.log(Level.WARNING, "Could not create Merkle tree node. " + e.getLocalizedMessage(), e);
            return null;
        }
    }

    private void storeMerkleTreeSnapShot() {
        append(merkleBTree.root);
    }

}
