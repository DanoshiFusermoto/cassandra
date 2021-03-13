package org.fuserleer.ledger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.Numbers;

@SerializerId2("ledger.state.field")
public final class StateField extends StateKey<Hash, String>
{
	private transient Hash cached = null;
	
	StateField()
	{
		super();
	}

	public StateField(final Hash scope, final String key)
	{
		super(scope, key);
		
		if (scope instanceof Hash) 
			Hash.notZero(scope, "Scope is ZERO");

		Numbers.notZero(key.length(), "Key length is zero");
	}

	@Override
	public synchronized Hash get()
	{
		if (this.cached == null)
			this.cached = new Hash(scope(), Hash.from(key()), Mode.STANDARD);

		return this.cached;
	}

	@Override
	void fromByteArray(byte[] bytes) throws IOException
	{
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		DataInputStream dis = new DataInputStream(bais);
		String key = dis.readUTF();
		byte[] scope = new byte[Hash.BYTES];
		dis.read(scope);
		key(key);
		scope(new Hash(scope));
	}

	@Override
	byte[] toByteArray() throws IOException
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		dos.writeUTF(key());
		dos.write(scope().toByteArray());
		return baos.toByteArray();
	}
}
