package org.fuserleer.ledger;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Objects;
import java.util.stream.Collectors;

import org.fuserleer.BasicObject;
import org.fuserleer.common.Primitive;
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
public final class Block extends BasicObject implements Primitive
{
	@JsonProperty("header")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private BlockHeader header;
	
	@JsonProperty("atoms")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private LinkedList<Atom> atoms;
	
	private Block()
	{
		super();
	}
	
	Block(final BlockHeader header, final Collection<Atom> atoms)
	{
		super();

		if (Objects.requireNonNull(atoms, "Atoms is null").isEmpty() == true)
			throw new IllegalArgumentException("Atoms is empty");
		
		this.header = Objects.requireNonNull(header, "Header is null");

		// TODO prevent duplicate atoms
		this.atoms = new LinkedList<Atom>(atoms);
	}

	public Block(long height, Hash previous, UInt256 stepped, long index, Hash merkle, ECPublicKey owner, Collection<Atom> atoms)
	{
		this(height, previous, stepped, index, merkle, Time.getLedgerTimeMS(), owner, atoms);
	}
	
	public Block(long height, Hash previous, UInt256 stepped, long index, Hash merkle, long timestamp, ECPublicKey owner, Collection<Atom> atoms)
	{
		super();

		if (Objects.requireNonNull(atoms, "Atoms is null").isEmpty() == true)
			throw new IllegalArgumentException("Atoms is empty");
		
		this.header = new BlockHeader(height, previous, stepped, index, atoms.stream().map(a -> a.getHash()).collect(Collectors.toList()), merkle, timestamp, owner);

		// TODO prevent duplicate atoms
		this.atoms = new LinkedList<Atom>(atoms);
	}

	protected synchronized Hash computeHash()
	{
		return this.header.getHash();
	}
	
	public boolean contains(Hash hash)
	{
		for (Atom atom : this.atoms)
		{
			if (atom.getHash().equals(hash) == true)
				return true;
			
			if (atom.hasParticle(hash) == true)
				return true;
		}
		
		return false;
	}
	
	public LinkedList<Atom> getAtoms()
	{
		return new LinkedList<Atom>(this.atoms);
	}

	public BlockHeader getHeader()
	{
		return this.header;
	}
}
