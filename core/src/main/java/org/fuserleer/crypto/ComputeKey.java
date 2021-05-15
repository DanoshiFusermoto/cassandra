package org.fuserleer.crypto;

import java.util.Arrays;
import java.util.Objects;

import org.fuserleer.utils.Base58;
import org.fuserleer.utils.Numbers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public final class ComputeKey extends Key
{
	public static final int	BYTES = Hash.BYTES;
	
	@JsonValue
	private final byte[] bytes;

	@JsonCreator
	public static ComputeKey from(byte[] key)
	{
		return new ComputeKey(key);
	}
	
	public static ComputeKey from(String key)
	{
		return new ComputeKey(key);
	}

	ComputeKey(String key)
	{
		this(Base58.fromBase58(Objects.requireNonNull(key, "Key string is null")));
	}

	private ComputeKey(byte[] key)  
	{
		Objects.requireNonNull(key, "Key bytes is null");
		Numbers.equals(key.length, Hash.BYTES, "Invalid HashKey size "+key.length);
		this.bytes = Arrays.copyOf(key, key.length);
	}

	public final Identity getIdentity()
	{
		return new Identity(Identity.COMPUTE, this);
	}
	
	@Override
	Hash computeHash()
	{
		return new Hash(this.bytes);
	}
	
	@Override
	public boolean canSign() 
	{
		return false;
	}

	@Override
	public boolean canVerify() 
	{
		return false;
	}

	public byte[] toByteArray() 
	{
		return this.bytes;
	}
}
