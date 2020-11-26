package org.fuserleer.crypto;

import java.security.InvalidParameterException;
import java.util.Objects;

public class MerkleNode implements Hashable
{
    private Hash hash;
    private MerkleNode leftNode;
    private MerkleNode rightNode;
    private MerkleNode parent;

    public MerkleNode() 
    {
    }

    public MerkleNode(Hash hash) 
    {
        this.hash = hash;
    }

    public MerkleNode(MerkleNode left, MerkleNode right) 
    {
        this.leftNode = left;
        this.rightNode = right;
        this.leftNode.parent = this;
        
        if (this.rightNode != null) 
        	this.rightNode.parent = this;

        computeHash();
    }

    public boolean isLeaf() 
    {
        return this.leftNode == null && this.rightNode == null;
    }

    @Override
    public String toString() 
    {
        return this.hash.toString();
    }

    public void setLeftNode(MerkleNode node) 
    {
        if (node.hash == null)
            throw new InvalidParameterException("Node hash must be initialized!");

        this.leftNode = node;
        this.leftNode.parent = this;

        computeHash();
    }

    public void setRightNode(MerkleNode node) 
    {
        if (node.hash == null)
            throw new InvalidParameterException("Node hash must be initialized!");

        this.rightNode = node;
        this.rightNode.parent = this;

        if (this.leftNode != null)
           computeHash();
    }

    public boolean canVerifyHash() 
    {
        return (this.leftNode != null && this.rightNode != null) || (this.leftNode != null);
    }

    public boolean verifyHash() 
    {
        if (this.leftNode == null && this.rightNode == null) return true;
        if (this.rightNode == null) return hash.equals(leftNode.hash);

        if (this.leftNode == null)
            throw new IllegalStateException("Left branch must be a node if right branch is a node!");

        Hash leftRightHash = new Hash(this.leftNode.hash, this.rightNode.hash, Hash.Mode.STANDARD);
        return hash.equals(leftRightHash);
    }

    public boolean equals(MerkleNode other) 
    {
        return this.hash.equals(other.hash);
    }

    @Override
    public Hash getHash() 
    {
        return this.hash;
    }

    public MerkleNode getParent() 
    {
        return this.parent;
    }

    public MerkleNode getLeftNode() 
    {
        return this.leftNode;
    }

    public MerkleNode getRightNode() 
    {
        return this.rightNode;
    }

    public void computeHash() 
    {
        if (this.rightNode == null) 
            this.hash = this.leftNode.hash;
        else 
            this.hash = new Hash(this.leftNode.hash, this.rightNode.hash, Hash.Mode.STANDARD);

        if (this.parent != null)
            this.parent.computeHash();
    }

    @Override
    public int hashCode() 
    {
        return Objects.hash(this.hash, this.leftNode, this.rightNode, this.parent);
    }
}
