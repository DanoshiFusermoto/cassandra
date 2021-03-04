package org.fuserleer.ledger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.state.address")
public final class StateAddress extends StateKey<Hash, Hash>
{
	private volatile Hash cached = null;
	
	StateAddress()
	{
		// FOR SERIALIZER 
		super();
	}

	public StateAddress(final Class<? extends Primitive> scope, final Hash key)
	{
		this(Hash.from(Serialization.getInstance().getIdForClass(Objects.requireNonNull(scope, "Class for scope is null"))), key);
	}

	public StateAddress(final Hash scope, final Hash key)
	{
		super(scope, key);
		Hash.notZero(scope, "Scope is ZERO");
	}

	public synchronized Hash get()
	{
		if (this.cached == null)
			this.cached = new Hash(scope(), key(), Mode.STANDARD);

		return this.cached;
	}
	
	@Override
	void fromByteArray(byte[] bytes) throws IOException
	{
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		DataInputStream dis = new DataInputStream(bais);
		byte[] key = new byte[Hash.BYTES];
		dis.read(key);
		byte[] scope = new byte[Hash.BYTES];
		dis.read(scope);
		key(new Hash(key));
		scope(new Hash(scope));
	}

	@Override
	byte[] toByteArray() throws IOException
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		dos.write(key().toByteArray());
		dos.write(scope().toByteArray());
		return baos.toByteArray();
	}

}
