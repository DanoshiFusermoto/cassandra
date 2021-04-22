package org.fuserleer.ledger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.fuserleer.Context;
import org.fuserleer.collections.Bloom;
import org.fuserleer.crypto.BLS12381;
import org.fuserleer.crypto.BLSPublicKey;
import org.fuserleer.crypto.BLSSignature;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hashable;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.BlockHeader.InventoryType;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.time.Time;
import org.fuserleer.utils.Numbers;

import com.google.common.primitives.Longs;

class PendingBlock implements Hashable
{
	private static final Logger blocksLog = Logging.getLogger("blocks");

	private final Context context;
	private	final long 	witnessed;

	private final Hash	hash;
	
	private Block 		block;
	private BlockHeader	header;
	private boolean 	unbranched;
	private Map<Hash, PendingAtom> atoms;
	private Map<Hash, AtomCertificate> certificates;
	private final Map<BLSPublicKey, BlockVote> votes;
	
	private final ReentrantLock lock = new ReentrantLock();

	public PendingBlock(final Context context, final Hash block)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.hash = Objects.requireNonNull(block);
		this.witnessed = Time.getLedgerTimeMS();
		this.unbranched = true;
		this.votes = new HashMap<BLSPublicKey, BlockVote>();
		this.atoms = new HashMap<Hash, PendingAtom>();
		this.certificates = new HashMap<Hash, AtomCertificate>();
	}

	public PendingBlock(final Context context, final BlockHeader header, final Collection<PendingAtom> atoms, final Collection<AtomCertificate> certificates)
	{
		this(context, Objects.requireNonNull(header, "Block header is null").getHash());
		
		Objects.requireNonNull(certificates, "Certificates is null");
		Objects.requireNonNull(atoms, "Pending atoms is null");
		Numbers.isZero(atoms.size(), "Pending atoms is empty");

		this.header = header;
		this.atoms.putAll(atoms.stream().collect(Collectors.toMap(pa -> pa.getHash(), pa -> pa)));
		this.certificates.putAll(certificates.stream().collect(Collectors.toMap(c -> c.getHash(), c -> c)));
		constructBlock();
	}
	
	@Override
	public Hash getHash()
	{
		return this.hash;
	}
	
	public long getHeight()
	{
		return Longs.fromByteArray(this.hash.toByteArray());
	}
	
	public boolean isUnbranched()
	{
		return this.unbranched;
	}
	
	public void setInBranch()
	{
		this.unbranched = false;
	}

	public BlockHeader getHeader()
	{
		this.lock.lock();
		try
		{
			return this.header;
		}
		finally
		{
			this.lock.unlock();
		}
	}
			
	void setHeader(final BlockHeader header)
	{
		Objects.requireNonNull(header, "Block header is null");

		this.lock.lock();
		try
		{
			if (this.header != null)
				throw new IllegalStateException("Header for "+this.hash+" is already present");
			
			if (Objects.requireNonNull(header, "Header is null").getHash().equals(this.hash) == false)
				throw new IllegalArgumentException("Expected header "+this.hash+" but received "+header.getHash());
			
			this.header = header;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	public Block getBlock()
	{
		this.lock.lock();
		try
		{
			return this.block;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	public void constructBlock()
	{
		this.lock.lock();
		try
		{
			if (this.block != null)
				return;
			
			List<Atom> atoms;
			List<Hash> atomInventory = this.header.getInventory(InventoryType.ATOMS);
			if (this.atoms.size() != atomInventory.size())
				return;
				
			atoms = new ArrayList<Atom>();
			for (Hash hash : atomInventory)
			{
				PendingAtom atom = this.atoms.get(hash);
				if (atom == null || atom.getAtom() == null)
				{
					if (blocksLog.hasLevel(Logging.DEBUG) == true)
						blocksLog.debug(this.context.getName()+": Atom "+hash+" not found for "+getHash());

					return;
				}
				
				atoms.add(atom.getAtom());
			}
			
			List<AtomCertificate> certificates;
			List<Hash> certificateInventory = this.header.getInventory(InventoryType.CERTIFICATES);
			certificates = new ArrayList<AtomCertificate>();
			for (Hash hash : certificateInventory)
			{
				AtomCertificate certificate = this.certificates.get(hash);
				if (certificate == null)
				{
					if (blocksLog.hasLevel(Logging.DEBUG) == true)
						blocksLog.debug(this.context.getName()+": Atom certificate "+hash+" not found for "+getHash());

					return;
				}
				
				certificates.add(certificate);
			}
			
			this.block = new Block(this.header, atoms, certificates);
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	boolean putAtom(final PendingAtom atom)
	{
		Objects.requireNonNull(atom, "Pending atom is null");
		this.lock.lock();
		try
		{
			if (this.atoms.containsKey(atom.getHash()) == false)
			{
				this.atoms.put(atom.getHash(), atom);
				return true;
			}
			
			return false;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	boolean putCertificate(final AtomCertificate certificate)
	{
		Objects.requireNonNull(certificate, "Certificate is null");

		this.lock.lock();
		try
		{
			if (this.certificates.containsKey(certificate.getHash()) == false)
			{
				this.certificates.put(certificate.getHash(), certificate);
				return true;
			}
			
			return false;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	boolean containsAtom(final Hash atom)
	{
		Objects.requireNonNull(atom, "Atom hash is null");
		Hash.notZero(atom, "Atom hash is ZERO");

		this.lock.lock();
		try
		{
			return this.atoms.containsKey(atom);
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	boolean containsCertificate(final Hash certificate)
	{
		Objects.requireNonNull(certificate, "Certificate hash is null");
		Hash.notZero(certificate, "Certificate hash is ZERO");
		
		this.lock.lock();
		try
		{
			return this.certificates.containsKey(certificate);
		}
		finally
		{
			this.lock.unlock();
		}
	}

	public PendingAtom getAtom(final Hash atom)
	{
		Objects.requireNonNull(atom, "Atom hash is null");
		Hash.notZero(atom, "Atom hash is zero");
		return this.atoms.get(atom);
	}

	public List<PendingAtom> getAtoms()
	{
		this.lock.lock();
		try
		{
			return Collections.unmodifiableList(this.atoms.values().stream().collect(Collectors.toList()));
		}
		finally
		{
			this.lock.unlock();
		}
	}

	public List<AtomCertificate> getCertificates()
	{
		this.lock.lock();
		try
		{
			return Collections.unmodifiableList(this.certificates.values().stream().collect(Collectors.toList()));
		}
		finally
		{
			this.lock.unlock();
		}
	}

	@Override
	public int hashCode()
	{
		return this.hash.hashCode();
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == null)
			return false;
		
		if (obj != this)
			return false;
		
		if ((obj instanceof PendingBlock) == true)
		{
			if (((PendingBlock)obj).hash.equals(this.hash) == true)
				return true;
		}
		
		return false;
	}

	@Override
	public String toString()
	{
		return (this.header == null ? this.hash : this.header.getHeight()+" "+this.header.getHash()+" "+this.header.getStep())+" @ "+this.witnessed;
	}
	
	public long getWitnessed()
	{
		return this.witnessed;
	}
	
	public boolean voted(final BLSPublicKey identity)
	{
		Objects.requireNonNull(identity, "Public key is null");
		
		this.lock.lock();
		try
		{
			return this.votes.containsKey(identity);
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	public BlockCertificate buildCertificate() throws CryptoException
	{
		this.lock.lock();
		try
		{
			if (this.header.getCertificate() != null)
			{
				blocksLog.warn("Block header already has a certificate "+this.header);
				return this.header.getCertificate();
			}
			
			BlockCertificate certificate;
			if (this.votes.isEmpty() == true)
			{
				blocksLog.warn("Block header has no votes "+this.header);
				certificate = new BlockCertificate(this.hash);
			}
			else
			{
				final Bloom signers = new Bloom(0.000001, this.votes.size());
				BLSSignature signature = BLS12381.aggregateSignatures(this.votes.values().stream().map(v -> {
					signers.add(v.getOwner().toByteArray());
					return v.getSignature();
				}).collect(Collectors.toList()));
				
				certificate = new BlockCertificate(this.hash, signers, signature);
			}

			this.header.setCertificate(certificate);
			return certificate;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	public boolean vote(final BlockVote vote) throws ValidationException
	{
		Objects.requireNonNull(vote, "Block vote is null");
		
		if (vote.getObject().equals(getHash()) == false)
			throw new ValidationException("Vote from "+vote.getOwner()+" is not for "+getHash());
		
		this.lock.lock();
		try
		{
			if (this.votes.containsKey(vote.getOwner()) == false)
			{
				this.votes.put(vote.getOwner(), vote);
				return true;
			}
			else
			{
				blocksLog.warn(this.context.getName()+": "+vote.getOwner()+" has already cast a vote for "+this.hash);
				return false;
			}
		}
		finally
		{
			this.lock.unlock();
		}
	}

	public Collection<BlockVote> votes()
	{
		this.lock.lock();
		try
		{
			return this.votes.values().stream().collect(Collectors.toList());
		}
		finally
		{
			this.lock.unlock();
		}
	}
}
