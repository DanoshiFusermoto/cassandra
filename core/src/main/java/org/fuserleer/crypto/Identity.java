package org.fuserleer.crypto;

import java.util.Objects;

import org.bouncycastle.util.Arrays;
import org.fuserleer.utils.Base58;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public final class Identity extends Key
{
	public static final byte EC = (byte)0x01;
	public static final byte BLS = (byte)0x02;
	public static final byte COMPUTE = (byte)0XFF;
	
	@JsonCreator
	public static Identity from(byte[] identity) throws CryptoException 
	{
		return new Identity(identity);
	}
	
	public static Identity from(String identity) throws CryptoException 
	{
		return new Identity(identity);
	}

	Identity(String identity) throws CryptoException
	{
		this(Base58.fromBase58(Objects.requireNonNull(identity, "Identity string is null")));
	}

	private Identity(byte[] identity) throws CryptoException 
	{
		Objects.requireNonNull(identity, "Identity bytes is null");
		
		this.prefix = identity[0];
		if (this.prefix == EC)
			this.key = ECPublicKey.from(Arrays.copyOfRange(identity, 1, identity.length));
		else if (this.prefix == BLS)
			this.key = BLSPublicKey.from(Arrays.copyOfRange(identity, 1, identity.length));
		else if (this.prefix == COMPUTE)
			this.key = ComputeKey.from(Arrays.copyOfRange(identity, 1, identity.length));
		else
			throw new IllegalArgumentException("Identity of type "+this.prefix+" is not supported for "+Base58.toBase58(identity));
	}

	
	private byte 	prefix; 
	private Key		key;
	
	public Identity(final byte prefix, final Key key)
	{
		Objects.requireNonNull(key, "Key is null");
		if (prefix != EC && prefix != BLS && prefix != COMPUTE)
			throw new IllegalArgumentException("Key prefix "+prefix+" is unknown");
		
		this.key = key;
		this.prefix = prefix;
	}

	public byte getPrefix()
	{
		return this.prefix;
	}

	public Key getKey()
	{
		return this.key;
	}

	@Override
	public boolean canSign()
	{
		return this.key.canSign();
	}

	@Override
	public boolean canVerify()
	{
		return this.key.canVerify();
	}

	@JsonValue
	@Override
	public byte[] toByteArray()
	{
		return Arrays.prepend(this.key.toByteArray(), this.prefix);
	}

	// MEH but can't really do anything about it!  An identity is a key and a key can be an identity
	@Override
	public final Identity getIdentity() 
	{
		return this;
	}
}
