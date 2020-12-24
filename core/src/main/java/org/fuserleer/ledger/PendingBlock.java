package org.fuserleer.ledger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.fuserleer.Context;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.ECSignatureBag;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hashable;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.time.Time;

class PendingBlock implements Hashable
{
	private static final Logger blocksLog = Logging.getLogger("blocks");

	private final Context context;
	private	final long 	witnessed;

	private Hash		hash;
	private Block		block;
	private long		voteWeight;
	private final Map<ECPublicKey, BlockVote> votes;

	public PendingBlock(Context context, Hash block)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.hash = Objects.requireNonNull(block);
		this.witnessed = Time.getLedgerTimeMS();
		this.voteWeight = 0l;
		this.votes = Collections.synchronizedMap(new HashMap<ECPublicKey, BlockVote>());
	}

	public PendingBlock(Context context, Block block)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.hash = Objects.requireNonNull(block).getHash();
		this.block = block;
		this.witnessed = Time.getLedgerTimeMS();
		this.voteWeight = 0l;
		this.votes = Collections.synchronizedMap(new HashMap<ECPublicKey, BlockVote>());
	}
	
	@Override
	public Hash getHash()
	{
		return this.hash;
	}
	
	public BlockHeader getBlockHeader()
	{
		return this.getBlock().getHeader();
	}
			
	public Block getBlock()
	{
		return this.block;
	}

	void setBlock(Block block)
	{
		this.block = Objects.requireNonNull(block);
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
		return (this.block == null ? this.hash : this.block.getHeader().getHeight()+" "+this.block.getHeader().getHash()+" "+this.block.getHeader().getStep())+" @ "+this.witnessed;
	}
	
	public long getWitnessed()
	{
		return this.witnessed;
	}
	
	public boolean voted(ECPublicKey identity)
	{
		return this.votes.containsKey(Objects.requireNonNull(identity, "Vote identity is null"));
	}
	
	public ECSignatureBag certificate()
	{
		if (getBlockHeader().getCertificate() != null)
		{
			blocksLog.warn("Block header already has a certificate "+getBlockHeader());
			return getBlockHeader().getCertificate();
		}
		
		synchronized(this.votes)
		{
			if (this.votes.isEmpty() == true)
				blocksLog.warn("Block header has no votes "+getBlockHeader());
			
			ECSignatureBag certificate = new ECSignatureBag(this.votes.values().stream().collect(Collectors.toMap(v -> v.getOwner(), v -> v.getSignature())));
			getBlockHeader().setCertificate(certificate);
			return certificate;
		}
	}

	public long vote(BlockVote vote, long weight) throws ValidationException
	{
		Objects.requireNonNull(vote, "Vote is null");
		Objects.requireNonNull(weight, "Weight is null");
		
		if (vote.getObject().equals(getHash()) == false)
			throw new ValidationException("Vote from "+vote.getOwner()+" is not for "+getHash());
		
		if (vote.getOwner().verify(vote.getHash(), vote.getSignature()) == false)
			throw new ValidationException("Signature from "+vote.getOwner()+" did not verify against "+getHash());

		synchronized(this.votes)
		{
			if (this.votes.containsKey(vote.getOwner()) == false)
			{
				this.votes.put(vote.getOwner(), vote);
				this.voteWeight += weight;
			}
			else
				blocksLog.warn(this.context.getName()+": "+vote.getOwner()+" has already cast a vote for "+this.hash);
			
		}

		return this.voteWeight;
	}
	
	public long weight()
	{
		return this.voteWeight;
	}

	public Collection<BlockVote> votes()
	{
		synchronized(this.votes)
		{
			return this.votes.values().stream().collect(Collectors.toList());
		}
	}
}
