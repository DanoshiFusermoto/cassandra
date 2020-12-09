package org.fuserleer.ledger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.MerkleProof;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerConstants;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.commit.indexable")
public final class IndexableCommit
{
	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;

	@JsonProperty("block")
	@DsonOutput(Output.ALL)
	private Hash block;
	
	@JsonProperty("atom")
	@DsonOutput(Output.ALL)
	private Hash atom;
	
	@JsonProperty("indexable")
	@DsonOutput(Output.ALL)
	private Hash indexable;

	@JsonProperty("timestamp")
	@DsonOutput(Output.ALL)
	private long timestamp;

	@JsonProperty("merkle_proof")
	@DsonOutput(Output.ALL)
	private List<MerkleProof> merkleProof;
	
	@SuppressWarnings("unused")
	private IndexableCommit()
	{
		// FOR SERIALIZER
		this.merkleProof = Collections.emptyList();
	}

	public IndexableCommit(byte[] bytes) throws IOException
	{
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes))
		{
			DataInputStream dis = new DataInputStream(bais);
			byte[] hashBytes = new byte[Hash.BYTES];
			dis.read(hashBytes);
			this.block = new Hash(hashBytes);
			dis.read(hashBytes);
			this.atom = new Hash(hashBytes);
			dis.read(hashBytes);
			this.indexable = new Hash(hashBytes);
			this.timestamp = dis.readLong();
			short merkleProofCount = dis.readShort();
			if (merkleProofCount > 0)
			{
				this.merkleProof = new ArrayList<MerkleProof>(merkleProofCount);
				for (int p = 0 ; p < merkleProofCount ; p++)
				{
					byte[] proofBytes = new byte[dis.readInt()];
					dis.read(proofBytes);
					this.merkleProof.add(new MerkleProof(proofBytes));
				}
			}
			else
				this.merkleProof = Collections.emptyList();
		}			
	}
	
	public IndexableCommit(final Hash block, final Hash atom, final Hash indexable, final List<MerkleProof> merkleProof, final long timestamp)
	{
		Objects.requireNonNull(block);
		if (block.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Block hash is ZERO");

		Objects.requireNonNull(atom);
		if (atom.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Atom hash is ZERO");

		Objects.requireNonNull(indexable);
		if (indexable.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Indexable hash is ZERO");
		
		Objects.requireNonNull(merkleProof);
//		if (merkleProof.isEmpty() == true)
//			throw new IllegalArgumentException("Merkle proof is empty");
		
		if (timestamp < 1)
			throw new IllegalArgumentException("Timestamp is negative");
		
		this.block = block;
		this.atom = atom;
		this.indexable = indexable;
		this.merkleProof = new ArrayList<MerkleProof>(merkleProof);
		this.timestamp = timestamp;
	}

	public Hash getBlock()
	{
		return this.block;
	}

	public Hash getAtom()
	{
		return this.atom;
	}

	public long getTimestamp()
	{
		return this.timestamp;
	}

	public Hash getIndexable()
	{
		return this.indexable;
	}

	public List<MerkleProof> getMerkleProofs()
	{
		return this.merkleProof;
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(this.block, this.indexable, this.merkleProof, this.timestamp);
	}

	@Override
	public boolean equals(Object other)
	{
		if (other == null)
			return false;
		if (other == this)
			return true;
		
		if (other instanceof IndexableCommit)
		{
			if (this.block.equals(((IndexableCommit)other).block) == false)
				return false;
			
			if (this.timestamp != ((IndexableCommit)other).timestamp)
				return false;

			if (this.indexable.equals(((IndexableCommit)other).indexable) == false)
				return false;
			
			if (this.merkleProof.equals(((IndexableCommit)other).merkleProof) == false)
				return false;
			
			return true;
		}
		
		return false;
	}

	@Override
	public String toString()
	{
		return this.block+" "+this.indexable+" "+" "+this.timestamp+" "+this.merkleProof;
	}
	
	byte[] toByteArray() throws IOException
	{
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream())
		{
			DataOutputStream dos = new DataOutputStream(baos);
			dos.write(this.block.toByteArray());
			dos.write(this.atom.toByteArray());
			dos.write(this.indexable.toByteArray());
			dos.writeLong(this.timestamp);
			dos.writeShort(this.merkleProof.size());
			for (MerkleProof merkleProof : this.merkleProof)
			{
				byte[] proofBytes = merkleProof.toByteArray();
				dos.writeInt(proofBytes.length); // TODO short should be enough here
				dos.write(proofBytes);
			}
			
			return baos.toByteArray();
		}	
	}
}