package org.fuserleer.crypto;

import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

public interface Hashable
{
	@JsonProperty("hash")
	@DsonOutput(Output.API)
	public default Hash getHash()
	{
		try
		{
			byte[] hashBytes = Serialization.getInstance().toDson(this, Output.HASH);
			return new Hash(hashBytes, Mode.DOUBLE);
		}
		catch (Exception e)
		{
			throw new RuntimeException("Error generating hash: " + e, e);
		}
	}
}
