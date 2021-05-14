package org.fuserleer.crypto;

import java.util.Arrays;
import java.util.Objects;

import org.fuserleer.utils.Bytes;
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
		this(Bytes.fromBase64String(Objects.requireNonNull(key, "Key string is null")));
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
