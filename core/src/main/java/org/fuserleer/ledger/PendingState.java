package org.fuserleer.ledger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantLock;

import org.fuserleer.Context;
import org.fuserleer.collections.Bloom;
import org.fuserleer.crypto.BLSPublicKey;
import org.fuserleer.crypto.BLSSignature;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.MerkleProof;
import org.fuserleer.crypto.MerkleProof.Branch;
import org.fuserleer.database.DatabaseException;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.utils.Longs;

final class PendingState
{
	private static final Logger statePoolLog = Logging.getLogger("statepool");

	private final Context context;
	
	private final Hash	atom;
	private final Hash	block;
	private final StateKey<?, ?> key;
	private final ReentrantLock lock = new ReentrantLock();

	private final Map<BLSPublicKey, StateVote> votes;
	private final Map<Hash, Long> weights;
	
	private Bloom signers;
	private StateVote majorityStateVote;
	private BLSPublicKey aggregatePublicKey;
	private BLSSignature aggregateSignature;
	private StateCertificate certificate;
	private boolean verified = false;
	private long agreedVotePower, totalVotePower;

	public PendingState(final Context context, final StateKey<?, ?> key, final Hash atom, final Hash block)
	{
		Objects.requireNonNull(key, "State key is null");
		Objects.requireNonNull(atom, "Atom is null");
		Hash.notZero(atom, "Atom hash is zero");
		Objects.requireNonNull(block, "Block is null");
		Hash.notZero(block, "Block hash is zero");

		this.context = context;
		this.key = key;
		this.atom = atom;
		this.block = block;
		this.votes = new HashMap<BLSPublicKey, StateVote>();
		this.weights = new HashMap<Hash, Long>();
	}

	public StateKey<?, ?> getKey()
	{
		return this.key;
	}
	
	public long getHeight()
	{
		return Longs.fromByteArray(this.block.toByteArray());
	}

	public Hash getBlock()
	{
		return this.block;
	}
			
	public Hash getAtom()
	{
		return this.atom;
	}
	
	public boolean preverify() throws DatabaseException
	{
		this.lock.lock();
		try
		{
			if (this.majorityStateVote != null)
				throw new IllegalStateException("Pending state "+this+" is already preverified");
			
			final long shardGroup = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), this.context.getLedger().numShardGroups(getHeight()));
			final long votePowerThreshold = this.context.getLedger().getValidatorHandler().getVotePowerThreshold(Math.max(0, getHeight() - ValidatorHandler.VOTE_POWER_MATURITY), shardGroup);
			final long totalVotePower = this.context.getLedger().getValidatorHandler().getTotalVotePower(Math.max(0, getHeight() - ValidatorHandler.VOTE_POWER_MATURITY), shardGroup);

			Entry<Hash, Long> executionWithMajority = null;
			for (Entry<Hash, Long> execution : this.weights.entrySet())
			{
				if (execution.getValue() >= votePowerThreshold)
				{
					executionWithMajority = execution;
					break;
				}
			}
			
			if (executionWithMajority == null)
				return false;
			
			// Aggregate and verify
			// TODO failures
			StateVote majorityStateVote = null;
			BLSPublicKey aggregatedPublicKey = null;
			BLSSignature aggregatedSignature = null;
			final Bloom signers = new Bloom(0.000001, this.votes.size());
			for (StateVote vote : this.votes.values())
			{
				if (vote.getExecution().equals(executionWithMajority.getKey()) == false)
					continue;
				
				if (majorityStateVote == null)
					majorityStateVote = new StateVote(vote.getState(), vote.getAtom(), vote.getBlock(), vote.getProducer(), vote.getInput(), vote.getOutput(), vote.getExecution());
				
				if (aggregatedPublicKey == null)
				{
					aggregatedPublicKey = vote.getOwner();
					aggregatedSignature = vote.getSignature();
				}
				else
				{
					aggregatedPublicKey = aggregatedPublicKey.combine(vote.getOwner());
					aggregatedSignature = aggregatedSignature.combine(vote.getSignature());
				}

				signers.add(vote.getOwner().toByteArray());
			}
			
			this.agreedVotePower = executionWithMajority.getValue();
			this.totalVotePower = totalVotePower;
			this.majorityStateVote = majorityStateVote;
			this.aggregatePublicKey = aggregatedPublicKey;
			this.aggregateSignature = aggregatedSignature;
			this.signers = signers;
			
			return true;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	public boolean isPreverified()
	{
		this.lock.lock();
		try
		{
			return this.majorityStateVote != null;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	public boolean verify() throws CryptoException
	{
		this.lock.lock();
		try
		{
			if (this.verified == true)
				throw new IllegalStateException("Pending state "+this+" majority is already verified");

			if (this.majorityStateVote == null)
				throw new IllegalStateException("Pending state "+this+" has not met threshold majority or is not prepared");
			
			if (this.aggregatePublicKey.verify(this.majorityStateVote.getTarget(), this.aggregateSignature) == false)
			{
				statePoolLog.error(this.context.getName()+": State pool votes failed verification for "+this.majorityStateVote.getTarget()+" of "+this.signers.count()+" aggregated signatures");
				this.majorityStateVote = null;
				this.verified = false;
				return false;
			}
			
			this.verified = true;
			return true;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	public boolean isVerified()
	{
		this.lock.lock();
		try
		{
			return this.verified;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	public StateCertificate buildCertificate() throws CryptoException, DatabaseException
	{
		this.lock.lock();
		try
		{
			if (this.certificate != null)
				throw new IllegalStateException("State certificate for "+this+" already exists");
			
			if (this.verified == false)
				throw new IllegalStateException("Pending state "+this+" is not verified");
			
			final long shardGroup = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), this.context.getLedger().numShardGroups(getHeight()));
			final VotePowerBloom votePowers = this.context.getLedger().getValidatorHandler().getVotePowerBloom(getBlock(), shardGroup);
			// TODO need merkles
			final StateCertificate certificate = new StateCertificate(this.majorityStateVote.getState(), this.majorityStateVote.getAtom(), 
																	  this.majorityStateVote.getBlock(), this.majorityStateVote.getProducer(), 
																	  this.majorityStateVote.getInput(), this.majorityStateVote.getOutput(), this.majorityStateVote.getExecution(), 
																      Hash.random(), Collections.singletonList(new MerkleProof(Hash.random(), Branch.OLD_ROOT)), votePowers, this.signers, this.aggregateSignature);
			this.certificate = certificate;
			
			if (statePoolLog.hasLevel(Logging.DEBUG) == true)
				statePoolLog.debug(this.context.getName()+": State certificate "+certificate.getHash()+" for state "+this.majorityStateVote.getState()+" in atom "+this.majorityStateVote.getAtom()+" has "+this.majorityStateVote.getDecision()+" agreement with "+this.agreedVotePower+"/"+this.totalVotePower);
			
			return this.certificate;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	public Collection<StateVote> votes()
	{
		this.lock.lock();
		try
		{
			return new ArrayList<StateVote>(this.votes.values());
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	boolean voted(final BLSPublicKey identity)
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


	boolean vote(final StateVote vote, long weight) throws ValidationException
	{
		this.lock.lock();
		try
		{
			if (vote.getAtom().equals(this.atom) == false || 
				vote.getBlock().equals(this.block) == false || 
				vote.getState().equals(this.key) == false)
				throw new ValidationException("Vote from "+vote.getOwner()+" is not for state "+this.key+" -> "+this.atom+" -> "+this.block);
				
			if (vote.getDecision().equals(StateDecision.NEGATIVE) == true && vote.getExecution().equals(Hash.ZERO) == false)
				throw new ValidationException("Vote from "+vote.getOwner()+" with decision "+vote.getDecision()+" for state "+this.key+" -> "+this.atom+" -> "+this.block+" is not of valid form");

			if (this.votes.containsKey(vote.getOwner()) == false)
			{
				this.votes.put(vote.getOwner(), vote);
				this.weights.compute(vote.getExecution(), (k, v) -> v == null ? weight : v + weight);
				return true;
			}
			else
				statePoolLog.warn(this.context.getName()+": "+vote.getOwner()+" has already cast a vote for "+vote.getState());
			
			return false;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	public StateCertificate getCertificate()
	{
		this.lock.lock();
		try
		{
			return this.certificate;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	@Override
	public int hashCode()
	{
		return this.key.hashCode();
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == null)
			return false;
		
		if (obj != this)
			return false;
		
		return true;
	}

	@Override
	public String toString()
	{
		return this.key+" @ "+this.atom+":"+getHeight();
	}
}
