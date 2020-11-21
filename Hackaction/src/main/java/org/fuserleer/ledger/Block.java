package org.fuserleer.ledger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.fuserleer.crypto.Hash;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.time.Time;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.block")
public final class Block extends BlockHeader implements Primitive, StatePrimitive
{
	@JsonProperty("atoms")
	@DsonOutput(Output.ALL)
	private List<Atom> atoms;
	
	@JsonProperty("witnessed_at")
	@DsonOutput(Output.PERSIST)
	private long witnessedAt;

	private Block()
	{
		super();
		
		this.witnessedAt = Time.getLedgerTimeMS();
	}
	
	public Block(long height, long step, Hash previous, Hash merkle, Collection<Atom> atoms)
	{
		this(height, step, previous, merkle, atoms, Time.getLedgerTimeMS());
	}
	
	Block(long height, long step, Hash previous, Hash merkle, Collection<Atom> atoms, long witnessedAt)
	{
		super(height, step, previous, merkle);

		if (witnessedAt < 0)
			throw new IllegalArgumentException("Witnessed timestamp is negative");
		
		this.atoms = new ArrayList<Atom>(Objects.requireNonNull(atoms, "Atoms is empty"));
		this.witnessedAt = witnessedAt;
	}

	public List<Atom> getAtoms()
	{
		return Collections.unmodifiableList(this.atoms);
	}

	public long getWitnessedAt()
	{
		return this.witnessedAt;
	}
}
