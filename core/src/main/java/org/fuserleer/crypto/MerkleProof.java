package org.fuserleer.crypto;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.SerializerConstants;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("crypto.merkle.proof")
public class MerkleProof implements Hashable
{
	public static final int BYTES = Hash.BYTES + Byte.BYTES; 
	
	public enum Branch 
	{
        LEFT,
        RIGHT,
        OLD_ROOT
    }

	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;

	@JsonProperty("hash")
	@DsonOutput(Output.ALL)
    public Hash hash;
	
	@JsonProperty("direction")
	@DsonOutput(Output.ALL)
    public Branch direction;
	
	@SuppressWarnings("unused")
	private MerkleProof()
	{
		// FOR SERIALIZER
	}
	
	public MerkleProof(byte[] bytes) throws IOException
	{
		ByteBuffer bai = ByteBuffer.wrap(bytes);
		byte[] hashBytes = new byte[Hash.BYTES];
		bai.get(hashBytes);
		this.hash = new Hash(hashBytes);
		this.direction = Branch.values()[bai.get()];
	}

    public MerkleProof(Hash hash, Branch direction) 
    {
        this.hash = hash;
        this.direction = direction;
    }

    @Override
    public Hash getHash() 
    {
        return this.hash;
    }

    public Branch getDirection() 
    {
        return this.direction;
    }

    @Override
    public String toString() 
    {
        String hash = this.hash.toString();
        String direction = this.direction.toString();
        return hash.concat("  is ".concat(direction).concat(" Child"));
    }
    
	public byte[] toByteArray() throws IOException
	{
		ByteBuffer bao = ByteBuffer.allocate(MerkleProof.BYTES);
		bao.put(this.hash.toByteArray());
		bao.put((byte) this.direction.ordinal());
		return bao.array();
	}
}
