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

    private static Logger log = Logger.getLogger(MerkleBTreeManager.class.getName());

    private static LinkedList<TreeNode> merkleTreeSnapshots = new LinkedList<>();
    private static MerkleBTree merkleBTree = new MerkleBTree();


    private static void append(TreeNode root) {
        merkleTreeSnapshots.add(root);
    }

    public static TreeNode getLatest() {
        return merkleTreeSnapshots.peekLast();
    }

    public static TreeNode.Nodes getHash(String key) {
        TreeNode.Nodes nodes = new TreeNode.Nodes();
        try {
            merkleBTree.get(key.getBytes(),nodes);
            return nodes;
        } catch (IOException e) {
            log.log(Level.WARNING, "Could not get hashes for key: "+key, e);
        }
        return null;
    }

    public static Bzs.MerkleTree insert(String key, Bzs.DBData data, boolean storeMBTreeSnapshot) {
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

    public static void storeMerkleTreeSnapShot() {
        append(merkleBTree.root);
    }

}
