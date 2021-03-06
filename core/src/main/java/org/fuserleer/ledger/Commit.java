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
import org.fuserleer.serialization.SerializerConstants;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.Numbers;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.commit")
public final class Commit 
{
	public static Commit from(final byte[] bytes) throws IOException
	{
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		DataInputStream dis = new DataInputStream(bais);
		long index = dis.readLong();
		
		byte[] pathBytes = new byte[dis.readShort()];
		dis.read(pathBytes);
		Path path = Path.from(pathBytes);
		
		boolean acceptTimedout = dis.readBoolean();
		boolean commitTimedout = dis.readBoolean();
		long timestamp = dis.readLong();
		
		int auditSize = dis.readShort();
		List<MerkleProof> audit = Collections.emptyList(); 
		if (auditSize > 0)
		{
			audit = new ArrayList<MerkleProof>(); 
			for (int a = 0 ; a < auditSize ; a++)
			{
				byte[] auditBytes = new byte[MerkleProof.BYTES];
				dis.read(auditBytes);
				audit.add(new MerkleProof(auditBytes));
			}
		}
		
		if (index == -1)
			return new Commit(path.endpoint(), timestamp);
		else
			return new Commit(index, path, audit, timestamp, acceptTimedout, commitTimedout);
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
	private Path path;

	@JsonProperty("timestamp")
	@DsonOutput(Output.ALL)
	private long timestamp;

	@JsonProperty("accept_timedout")
	@DsonOutput(Output.ALL)
	private boolean acceptTimedout;

	@JsonProperty("commit_timedout")
	@DsonOutput(Output.ALL)
	private boolean commitTimedout;

	@JsonProperty("merkle_proof")
	@DsonOutput(Output.ALL)
	private List<MerkleProof> merkleProof;
	
	private Commit()
	{
		// FOR SERIALIZER
		this.merkleProof = Collections.emptyList();
	}

	public Commit(final Hash endpoint, final long timestamp)
	{
		this();
		Numbers.isNegative(timestamp, "Timestamp is negative");
		
		this.index = -1;
		this.path = new Path(endpoint);
		this.commitTimedout = false;
		this.acceptTimedout = false;
		this.timestamp = timestamp;
		this.merkleProof = Collections.emptyList();
	}

	public Commit(final long index, final Path path, final List<MerkleProof> merkleProof, final long timestamp, final boolean acceptTimedout, final boolean commitTimedout)
	{
		Objects.requireNonNull(path);
		Objects.requireNonNull(merkleProof);
		Numbers.isNegative(index , "Index is negative");
		Numbers.isNegative(timestamp, "Timestamp is negative");
		
		this.index = index;
		this.path = path;
		this.merkleProof = new ArrayList<MerkleProof>(merkleProof);
		this.acceptTimedout = acceptTimedout;
		this.commitTimedout = commitTimedout;
		this.timestamp = timestamp;
	}
	
/*	private void validate()
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
	}*/

	public long getIndex()
	{
		return this.index;
	}
	
	public boolean isAcceptTimedout()
	{
		return this.acceptTimedout;
	}
	
	void setAcceptTimedOut()
	{
		this.acceptTimedout = true;
	}

	public boolean isCommitTimedout()
	{
		return this.commitTimedout;
	}
	
	void setCommitTimedOut()
	{
		this.commitTimedout = true;
	}

	public long getTimestamp()
	{
		return this.timestamp;
	}

	public Path getPath()
	{
		return this.path;
	}

	public List<MerkleProof> getMerkleProofs()
	{
		return this.merkleProof;
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(this.index, this.path, this.merkleProof, this.timestamp);
	}

	@Override
	public boolean equals(Object other)
	{
		if (other == null)
			return false;
		if (other == this)
			return true;
		
		if (other instanceof Commit)
		{
			if (this.index != ((Commit)other).index)
				return false;

			if (this.acceptTimedout != ((Commit)other).acceptTimedout)
				return false;

			if (this.commitTimedout != ((Commit)other).commitTimedout)
				return false;

			if (this.timestamp != ((Commit)other).timestamp)
				return false;

			if (this.path.equals(((Commit)other).path) == false)
				return false;
			
			if (this.merkleProof.equals(((Commit)other).merkleProof) == false)
				return false;
			
			return super.equals(other);
		}
		
		return false;
	}

	@Override
	public String toString()
	{
		return this.index+" "+this.path+" "+" "+this.timestamp+" "+this.acceptTimedout+":"+this.commitTimedout+" "+this.merkleProof;
	}
	
	public final byte[] toByteArray() throws IOException
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		dos.writeLong(this.index);
		
		byte[] pathBytes = this.path.toByteArray();
		dos.writeShort(pathBytes.length);
		dos.write(pathBytes);
		dos.writeBoolean(this.acceptTimedout);
		dos.writeBoolean(this.commitTimedout);
		dos.writeLong(this.timestamp);
		
		int auditSize = this.merkleProof.size();
		dos.writeShort(auditSize);
		if (auditSize > 0)
		{
			for (MerkleProof merkleProof : this.merkleProof)
				dos.write(merkleProof.toByteArray());
		}
		return baos.toByteArray();
	}
}