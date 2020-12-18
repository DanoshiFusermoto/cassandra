package org.fuserleer.ledger;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.fuserleer.BasicObject;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Certificate;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.MerkleTree;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.time.Time;
import org.fuserleer.utils.UInt256;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@SerializerId2("ledger.block")
public final class Block extends BasicObject implements Primitive
{
	@JsonProperty("header")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private BlockHeader header;
	
	@JsonProperty("atoms")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private LinkedList<Atom> atoms;
	
	@JsonProperty("certificates")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	@JsonDeserialize(as=LinkedHashMap.class)
	private Map<Hash, Certificate> certificates;

	private Block()
	{
		super();
	}
	
	Block(final BlockHeader header, final Collection<Atom> atoms, final Collection<Certificate> certificates)
	{
		super();

		if (Objects.requireNonNull(atoms, "Atoms is null").isEmpty() == true)
			throw new IllegalArgumentException("Atoms is empty");
		
		this.header = Objects.requireNonNull(header, "Header is null");

		// TODO prevent duplicate atoms
		this.atoms = new LinkedList<Atom>(atoms);

		this.certificates = new LinkedHashMap<Hash, Certificate>();
		for (Certificate certificate : Objects.requireNonNull(certificates, "Certificates is null"))
			this.certificates.put(certificate.getObject(), certificate);
	}

	public Block(final long height, final Hash previous, final UInt256 stepped, final long index, final ECPublicKey owner, final Collection<Atom> atoms, final Collection<Certificate> certificates)
	{
		this(height, previous, stepped, index, Time.getLedgerTimeMS(), owner, atoms, certificates);
	}
	
	public Block(final long height, final Hash previous, final UInt256 stepped, final long index, final long timestamp, final ECPublicKey owner, final Collection<Atom> atoms, final Collection<Certificate> certificates)
	{
		super();

		if (Objects.requireNonNull(atoms, "Atoms is null").isEmpty() == true)
			throw new IllegalArgumentException("Atoms is empty");
		
		// TODO prevent duplicate atoms
		this.atoms = new LinkedList<Atom>(atoms);
		
		this.certificates = new LinkedHashMap<Hash, Certificate>();
		for (Certificate certificate : Objects.requireNonNull(certificates, "Certificates is null"))
			this.certificates.put(certificate.getObject(), certificate);
		
		final MerkleTree merkle = new MerkleTree();
		this.atoms.forEach(a -> merkle.appendLeaf(a.getHash()));
		this.certificates.values().forEach(c -> merkle.appendLeaf(c.getHash()));
		
		final Map<Class<? extends Primitive>, List<Hash>> inventory = new HashMap<>();
		inventory.put(Atom.class, this.atoms.stream().map(a -> a.getHash()).collect(Collectors.toList()));
		inventory.put(Certificate.class, this.certificates.values().stream().map(c -> c.getHash()).collect(Collectors.toList()));
		
		this.header = new BlockHeader(height, previous, stepped, index, inventory, merkle.buildTree(), timestamp, owner);
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
