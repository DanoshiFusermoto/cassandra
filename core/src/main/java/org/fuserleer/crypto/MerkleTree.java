package org.fuserleer.crypto;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class MerkleTree
{
    private MerkleNode root;
    private List<MerkleNode> nodes;
    private List<MerkleNode> leaves;

    public MerkleTree() 
    {
        this.nodes = new ArrayList<>();
        this.leaves = new ArrayList<>();
    }

    public List<MerkleNode> getLeaves() 
    {
        return this.leaves;
    }
    
    public List<MerkleNode> getNodes() 
    {
        return this.nodes;
    }
    
    public MerkleNode getRoot() 
    {
        return this.root;
    }

    public MerkleNode appendLeaf(MerkleNode node) 
    {
        this.nodes.add(node);
        this.leaves.add(node);
        return node;
    }

    public void appendLeaves(MerkleNode[] nodes) 
    {
        for (MerkleNode node : nodes)
            this.appendLeaf(node);
    }

    public MerkleNode appendLeaf(Hash hash) 
    {
        return this.appendLeaf(new MerkleNode(hash));
    }

    public List<MerkleNode> appendLeaves(Hash[] hashes) 
    {
        List<MerkleNode> nodes = new ArrayList<>();
        for (Hash hash : hashes)
            nodes.add(this.appendLeaf(hash));

        return nodes;
    }

    public List<MerkleNode> appendLeaves(Collection<Hash> hashes) 
    {
        List<MerkleNode> nodes = new ArrayList<>();
        for (Hash hash : hashes)
            nodes.add(this.appendLeaf(hash));

        return nodes;
    }

    public Hash addTree(MerkleTree tree) 
    {
        if (this.leaves.size() <= 0) 
        	throw new IllegalStateException("Cannot add to a tree with no leaves!");
        
        tree.leaves.forEach(this::appendLeaf);
        return this.buildTree();
    }

    public Hash buildTree() 
    {
        if (this.leaves.size() <= 0) 
        	throw new IllegalStateException("Cannot add to a tree with no leaves!");
        
        buildTree(this.leaves);
        return this.root.getHash();
    }

    public void buildTree(List<MerkleNode> nodes) 
    {
        if (nodes.size() <= 0) 
        	throw new IllegalStateException("Node list not expected to be empty!");

        if (nodes.size() == 1) 
        {
            this.root = nodes.get(0);
        } 
        else 
        {
            List<MerkleNode> parents = new ArrayList<>();
            for (int i = 0; i < nodes.size(); i += 2) 
            {
                MerkleNode right = (i + 1 < nodes.size()) ? nodes.get(i + 1) : null;
                MerkleNode parent = new MerkleNode(nodes.get(i), right);
                parents.add(parent);
            }
            
            buildTree(parents);
        }
    }

    public List<MerkleProof> auditProof(Hash leafHash) 
    {
        List<MerkleProof> auditTrail = new ArrayList<>();

        MerkleNode leafNode = findLeaf(leafHash);
        if (leafNode != null) 
        {
            if (leafNode.getParent() == null) 
            	throw new IllegalStateException("Expected leaf to have a parent!");
            
            MerkleNode parent = leafNode.getParent();
            buildAuditTrail(auditTrail, parent, leafNode);
        }

        return auditTrail;
    }

    public static boolean verifyAudit(Hash rootHash, Hash leafHash, List<MerkleProof> auditTrail) 
    {
        if (auditTrail.size() <= 0) 
        	throw new IllegalStateException("Audit trail cannot be empty!");

        Hash testHash = leafHash;

        for (MerkleProof auditHash : auditTrail) 
            testHash = auditHash.direction.equals(MerkleProof.Branch.RIGHT) ? new Hash(testHash, auditHash.hash, Hash.Mode.STANDARD) : new Hash(auditHash.hash, testHash, Hash.Mode.STANDARD);

        return testHash.equals(rootHash);
    }

    private MerkleNode findLeaf(Hash hash) 
    {
        return this.leaves.stream().filter((leaf) -> leaf.getHash().equals(hash)).findFirst().orElse(null);
    }

    private void buildAuditTrail(List<MerkleProof> auditTrail, MerkleNode parent, MerkleNode child) 
    {
        if (parent != null) 
        {
            if (child.getParent() != parent)
                throw new IllegalStateException("Parent of child is not expected parent!");

            MerkleNode nextChild = parent.getLeftNode().equals(child) ? parent.getRightNode() : parent.getLeftNode();
            MerkleProof.Branch direction = parent.getLeftNode().equals(child) ? MerkleProof.Branch.RIGHT : MerkleProof.Branch.LEFT;

            if (nextChild != null) 
            	auditTrail.add(new MerkleProof(nextChild.getHash(), direction));

            buildAuditTrail(auditTrail, parent.getParent(), child.getParent());
        }
    }
}