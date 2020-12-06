package org.fuserleer.ledger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.fuserleer.collections.Bloom;
import org.fuserleer.common.Primitive;
import org.fuserleer.common.StatePrimitive;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.time.Time;
import org.fuserleer.utils.UInt256;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.block")
public final class Block extends BlockHeader implements Primitive, StatePrimitive
{
	@JsonProperty("atoms")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private List<Atom> atoms;
	
	private Block()
	{
		super();
	}
	
	public Block(long height, Hash previous, UInt256 stepped, Bloom bloom, Hash merkle, ECPublicKey owner, Collection<Atom> atoms)
	{
		this(height, previous, stepped, bloom, merkle, Time.getLedgerTimeMS(), owner, atoms);
	}
	
	public Block(long height, Hash previous, UInt256 stepped, Bloom bloom, Hash merkle, long timestamp, ECPublicKey owner, Collection<Atom> atoms)
	{
		super(height, previous, stepped, bloom, merkle, timestamp, owner);

		if (Objects.requireNonNull(atoms, "Atoms is null").isEmpty() == true)
			throw new IllegalArgumentException("Atoms is empty");

		// TODO prevent duplicate atoms
		this.atoms = new ArrayList<Atom>(atoms);
	}

	public List<Atom> getAtoms()
	{
		return Collections.unmodifiableList(this.atoms);
	}

	public BlockHeader toHeader()
	{
		return super.clone();
	}
}
