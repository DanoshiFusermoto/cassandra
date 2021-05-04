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
import org.fuserleer.crypto.BLSPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.MerkleTree;
import org.fuserleer.ledger.BlockHeader.InventoryType;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.time.Time;
import org.fuserleer.utils.Numbers;
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
	
	@JsonProperty("timedout")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private LinkedList<Atom> timedout;

	@JsonProperty("certificates")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	@JsonDeserialize(as=LinkedHashMap.class)
	private Map<Hash, AtomCertificate> certificates;

	@SuppressWarnings("unused")
	private Block()
	{
		super();
	}
	
	Block(final BlockHeader header, final Collection<Atom> atoms, final Collection<AtomCertificate> certificates, final Collection<Atom> timedout)
	{
		super();

		Objects.requireNonNull(certificates, "Certificates is null");
		Objects.requireNonNull(timedout, "Timedout is null");
		Objects.requireNonNull(header, "Header is null");
		Objects.requireNonNull(atoms, "Atoms is null");
		Numbers.isZero(atoms.size(), "Atoms is empty");

		this.header = header;

		// TODO prevent duplicate atoms
		//		allowed currently to allow testing of duplicate atom injections which should fail during consensus
		this.atoms = new LinkedList<Atom>(atoms);
		this.timedout = new LinkedList<Atom>(timedout);
		this.certificates = new LinkedHashMap<Hash, AtomCertificate>();
		for (AtomCertificate certificate : certificates)
			this.certificates.put(certificate.getAtom(), certificate);
	}

	public Block(final long height, final Hash previous, final long target, final UInt256 stepped, final long index, final BLSPublicKey owner, 
			     final Collection<Atom> atoms, final Collection<AtomCertificate> certificates, final Collection<Atom> timedout)
	{
		this(height, previous, target, stepped, index, Time.getLedgerTimeMS(), owner, atoms, certificates, timedout);
	}
	
	public Block(final long height, final Hash previous, final long target, final UInt256 stepped, final long index, final long timestamp, final BLSPublicKey owner, 
				 final Collection<Atom> atoms, final Collection<AtomCertificate> certificates, final Collection<Atom> timedout)
	{
		super();

		Objects.requireNonNull(certificates, "Certificates is null");
		Objects.requireNonNull(timedout, "Timeouts is null");
		Objects.requireNonNull(atoms, "Atoms is null");
		Numbers.isZero(atoms.size(), "Atoms is empty");
		
		// TODO prevent duplicate atoms
		//		allowed currently to allow testing of duplicate atom injections which should fail during consensus
		this.atoms = new LinkedList<Atom>(atoms);
		this.timedout = new LinkedList<Atom>(timedout);
		this.certificates = new LinkedHashMap<Hash, AtomCertificate>();
		for (AtomCertificate certificate : certificates)
			this.certificates.put(certificate.getObject(), certificate);
		
		final MerkleTree merkle = new MerkleTree();
		this.atoms.forEach(a -> merkle.appendLeaf(a.getHash()));
		this.certificates.values().forEach(c -> merkle.appendLeaf(c.getHash()));
		
		final Map<InventoryType, List<Hash>> inventory = new HashMap<>();
		inventory.put(InventoryType.ATOMS, this.atoms.stream().map(a -> a.getHash()).collect(Collectors.toList()));
		inventory.put(InventoryType.CERTIFICATES, this.certificates.values().stream().map(c -> c.getHash()).collect(Collectors.toList()));
		inventory.put(InventoryType.TIMEOUTS, this.timedout.stream().map(t -> t.getHash()).collect(Collectors.toList()));
		
		this.header = new BlockHeader(height, previous, target, stepped, index, inventory, merkle.buildTree(), timestamp, owner);
	}

	protected synchronized Hash computeHash()
	{
		return this.header.getHash();
	}
	
	public boolean contains(final Hash hash)
	{
		Objects.requireNonNull(hash, "Hash is null");
		Hash.notZero(hash, "Hash is zero");
		
		for (Atom atom : this.atoms)
		{
			if (atom.getHash().equals(hash) == true)
				return true;
			
			if (atom.hasParticle(hash) == true)
				return true;
		}
		
		if (this.timedout != null)
		{
			for (Atom atom : this.timedout)
			{
				if (atom.getHash().equals(hash) == true)
					return true;
				
				if (atom.hasParticle(hash) == true)
					return true;
			}
		}

		return false;
	}
	
	public BlockHeader getHeader()
	{
		return this.header;
	}


	public LinkedList<Atom> getAtoms()
	{
		return new LinkedList<Atom>(this.atoms);
	}

	public LinkedList<Atom> getTimedout()
	{
		if (this.timedout == null || this.timedout.isEmpty() == true)
			return new LinkedList<Atom>();

		return new LinkedList<Atom>(this.timedout);
	}

	public LinkedList<AtomCertificate> getCertificates()
	{
		if (this.certificates == null || this.certificates.isEmpty() == true)
			return new LinkedList<AtomCertificate>();
		
		return new LinkedList<AtomCertificate>(this.certificates.values());
	}
}
