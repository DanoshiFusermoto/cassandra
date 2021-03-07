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
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.ECSignatureBag;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hashable;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.BlockHeader.InventoryType;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.time.Time;

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
	
	private long  voteWeight;
	private final Map<ECPublicKey, BlockVote> votes;
	
	private final ReentrantLock lock = new ReentrantLock();

	public PendingBlock(final Context context, final Hash block)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.hash = Objects.requireNonNull(block);
		this.witnessed = Time.getLedgerTimeMS();
		this.voteWeight = 0l;
		this.unbranched = true;
		this.votes = new HashMap<ECPublicKey, BlockVote>();
		this.atoms = new HashMap<Hash, PendingAtom>();
		this.certificates = new HashMap<Hash, AtomCertificate>();
	}

	public PendingBlock(final Context context, final BlockHeader header, final Collection<PendingAtom> atoms, final Collection<AtomCertificate> certificates)
	{
		this(context, Objects.requireNonNull(header).getHash());
		
		Objects.requireNonNull(certificates, "Certificates is null");
		Objects.requireNonNull(atoms, "Pending atoms is null");

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
			
	void setHeader(BlockHeader header)
	{
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
	
	boolean putAtom(PendingAtom atom)
	{
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
	
	boolean putCertificate(AtomCertificate certificate)
	{
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
	
	public boolean voted(ECPublicKey identity)
	{
		this.lock.lock();
		try
		{
			return this.votes.containsKey(Objects.requireNonNull(identity, "Vote identity is null"));
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	public ECSignatureBag certificate()
	{
		this.lock.lock();
		try
		{
			if (this.header.getCertificate() != null)
			{
				blocksLog.warn("Block header already has a certificate "+this.header);
				return this.header.getCertificate();
			}
			
			ECSignatureBag certificate;
			if (this.votes.isEmpty() == true)
			{
				blocksLog.warn("Block header has no votes "+this.header);
				certificate = new ECSignatureBag();
			}
			else
				certificate = new ECSignatureBag(this.votes.values().stream().collect(Collectors.toMap(v -> v.getOwner(), v -> v.getSignature())));

			this.header.setCertificate(certificate);
			return certificate;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	public long vote(BlockVote vote, long weight) throws ValidationException
	{
		Objects.requireNonNull(vote, "Vote is null");
		
		if (vote.getObject().equals(getHash()) == false)
			throw new ValidationException("Vote from "+vote.getOwner()+" is not for "+getHash());
		
		if (vote.getOwner().verify(vote.getHash(), vote.getSignature()) == false)
			throw new ValidationException("Signature from "+vote.getOwner()+" did not verify against "+getHash());
		
		if (weight == 0)
			blocksLog.warn(this.context.getName()+": Weight is 0 for "+vote.getOwner()+" block vote "+vote.getHash()+":"+vote.getBlock()+":"+vote.getDecision());

		this.lock.lock();
		try
		{
			if (this.votes.containsKey(vote.getOwner()) == false)
			{
				this.votes.put(vote.getOwner(), vote);
				this.voteWeight += weight;
			}
			else
				blocksLog.warn(this.context.getName()+": "+vote.getOwner()+" has already cast a vote for "+this.hash);

			return this.voteWeight;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	public long weight()
	{
		this.lock.lock();
		try
		{
			return this.voteWeight;
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
