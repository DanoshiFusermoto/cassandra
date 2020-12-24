package org.fuserleer.ledger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.fuserleer.crypto.Certificate;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.MerkleProof;
import org.fuserleer.database.Indexable;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.Particle;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerConstants;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@SerializerId2("ledger.commit.indexable")
public final class IndexableCommit
{
	public enum Path
	{
		BLOCK,
		ATOM,
		CERTIFICATE;

		@JsonValue
		@Override
		public String toString() 
		{
			return this.name();
		}
	}

	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;

	@JsonProperty("index")
	@DsonOutput(Output.ALL)
	private long index;

	@JsonProperty("path")
	@DsonOutput(Output.ALL)
	@JsonDeserialize(as=LinkedHashMap.class)
	private Map<Path, Hash> path;
	
	@JsonProperty("indexable")
	@DsonOutput(Output.ALL)
	private Indexable indexable;

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

	public IndexableCommit(final long index, final Indexable indexable, final List<MerkleProof> merkleProof, final long timestamp, final Indexable ... path)
	{
		Objects.requireNonNull(indexable);
		Objects.requireNonNull(merkleProof);
//		if (merkleProof.isEmpty() == true)
//			throw new IllegalArgumentException("Merkle proof is empty");
		
		if (timestamp < 1)
			throw new IllegalArgumentException("Timestamp is negative");
		
		if (index < 0)
			throw new IllegalArgumentException("Index is negative");

		this.index = index;
		this.indexable = indexable;
		this.merkleProof = new ArrayList<MerkleProof>(merkleProof);
		this.timestamp = timestamp;
		this.path = new LinkedHashMap<>();
		if (path != null)
		{
			for (Indexable step : path)
			{
				if (BlockHeader.class.isAssignableFrom(step.getContainer()) == true)
					this.path.put(Path.BLOCK, step.getKey());
				else if (Certificate.class.isAssignableFrom(step.getContainer()) == true)
					this.path.put(Path.CERTIFICATE, step.getKey());
				else if (Atom.class.isAssignableFrom(step.getContainer()) == true)
					this.path.put(Path.ATOM, step.getKey());
				else
					throw new IllegalArgumentException("Path element "+step.getContainer()+" is not supported");
			}
		}
		
		validatePath();
	}
	
	private void validatePath()
	{
		if (BlockHeader.class.isAssignableFrom(this.indexable.getContainer()) == true)
			if (this.path.isEmpty() == false)
				throw new IllegalStateException("Block indexable commits can not have a path");

		if (Atom.class.isAssignableFrom(this.indexable.getContainer()) == true)
		{
			if (this.path.size() == 1 && this.path.containsKey(Path.BLOCK) == false)
				throw new IllegalStateException("Atom indexable pre-commit must have a Block path element");
			else if (this.path.size() == 2 && (this.path.containsKey(Path.BLOCK) == false || this.path.containsKey(Path.CERTIFICATE) == false))
				throw new IllegalStateException("Atom indexable commit must have a Block and Certificate path element");
			else if (this.path.size() == 0 || this.path.size() > 2)
				throw new IllegalStateException("Atom indexable commit is invalid");
		}

		if (Particle.class.isAssignableFrom(this.indexable.getContainer()) == true)
		{
			if (this.path.size() != 2 || this.path.containsKey(Path.BLOCK) == false || this.path.containsKey(Path.ATOM) == false)
				throw new IllegalStateException("Padticle indexable commits must have both Block and Atom path elements");
		}

		if (Certificate.class.isAssignableFrom(this.indexable.getContainer()) == true)
		{
			if (this.path.size() != 1 || this.path.containsKey(Path.BLOCK) == false)
				throw new IllegalStateException("Atom indexable commits with certificates must have only a Block path element");
		}
	}

	public long getIndex()
	{
		return this.index;
	}

	public Hash get(Path type)
	{
		return this.path.get(type);
	}

	public long getTimestamp()
	{
		return this.timestamp;
	}

	public Indexable getIndexable()
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
		return Objects.hash(this.index, this.path, this.indexable, this.merkleProof, this.timestamp);
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
			if (this.path.equals(((IndexableCommit)other).path) == false)
				return false;
			
			if (this.index != ((IndexableCommit)other).index)
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
		return this.index+" "+this.path+" "+this.indexable+" "+" "+this.timestamp+" "+this.merkleProof;
	}
}