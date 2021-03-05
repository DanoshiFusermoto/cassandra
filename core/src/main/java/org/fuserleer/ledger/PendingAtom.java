package org.fuserleer.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.fuserleer.Context;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hashable;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.CommitOperation.Type;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.ledger.events.AtomCertificateEvent;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.time.Time;
import org.fuserleer.utils.Longs;
import org.fuserleer.utils.UInt256;


/** 
 * Represents an Atom that is currently being processed.
 * <br><br>
 * Also acts as a StateMachine wrapper.
 */
public final class PendingAtom implements Hashable
{
	private static final Logger atomsLog = Logging.getLogger("atoms");
	private static final Logger cerbyLog = Logging.getLogger("cerby");

	public final static int ATOM_COMMIT_TIMEOUT_BLOCKS = 30;	// TODO need a much smarter timeout mechanism along with recovery.  ~10 minutes per phase if block production is ~5 seconds.  Sufficient for alpha testing.
	public final static int ATOM_INCLUSION_TIMEOUT_CLOCK_SECONDS = 600;	// TODO Long inclusion commit timeout ~10 minutes.  Is extended if atom is suspected included in a block (evidence of state votes etc).  Sufficient for alpha testing.
	
	public static PendingAtom create(final Context context, final Atom atom)
	{
		return new PendingAtom(context, atom);
	}

	public static PendingAtom create(final Context context, final Hash atom)
	{
		return new PendingAtom(context, atom);
	}
	
	private final Context 	context;
	private final Hash		hash;
	private	final long 		witnessed;
	
	/** The wall clock timeout if not included in a block for commit WARN subjective **/
	private	long 			inclusionTimeout;
	private long 			inclusionDelay;
	
	/** The block at which a commit timeout happens **/
	private	long 			commitTimeout;
	private Hash			block;
	private Atom 			atom;
	private StateMachine	stateMachine;
	
	private long			voteWeight;
	private long			voteThreshold;
	private final Set<AtomVote> queued;
	private final Set<AtomVote> votes;
	private final Map<ECPublicKey, Long> voted;
	
	private AtomCertificate certificate;
	private final Map<StateKey<?, ?>, StateCertificate> certificates;
	
	private final AtomicReference<CommitStatus>	status;
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

	private PendingAtom(final Context context, final Atom atom)
	{
		this(context, Objects.requireNonNull(atom, "Atom is null").getHash());

		setAtom(atom);
	}

	private PendingAtom(final Context context, final Hash atom)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		
		this.hash = Objects.requireNonNull(atom, "Atom is null");
		this.witnessed = Time.getSystemTime();
		this.commitTimeout = 0;
		this.inclusionTimeout = this.witnessed + TimeUnit.SECONDS.toMillis(PendingAtom.ATOM_INCLUSION_TIMEOUT_CLOCK_SECONDS);
		this.inclusionDelay = 0;
		this.voteWeight = 0l;
		this.voteThreshold = 0l;
		this.queued = new HashSet<AtomVote>();
		this.votes = new HashSet<AtomVote>();
		this.voted = new HashMap<ECPublicKey, Long>();
		this.certificates = new HashMap<StateKey<?, ?>, StateCertificate>();
		this.status = new AtomicReference<CommitStatus>(CommitStatus.NONE);
	}
	
	@Override
	public Hash getHash()
	{
		return this.hash;
	}
	
	public long getCommitTimeout()
	{
		return this.commitTimeout;
	}

	private void setCommitTimeout(long commitTimeout)
	{
		if (this.block == null)
			throw new IllegalStateException("Can not set commit timeout "+commitTimeout+" without being included in a block");
		
		if (commitTimeout < this.commitTimeout)
			throw new IllegalArgumentException(commitTimeout+" is less than current commit block timeout "+this.commitTimeout);
		
		this.commitTimeout = commitTimeout;
	}

	public long getWitnessed()
	{
		return this.witnessed;
	}

	public long getInclusionTimeout()
	{
		return this.inclusionTimeout;
	}

	private void setInclusionTimeout(long inclusionTimeout)
	{
		if (inclusionTimeout < this.inclusionTimeout)
			throw new IllegalArgumentException(inclusionTimeout+" is less than current inclusion timeout "+this.inclusionTimeout);
		
		this.inclusionTimeout = inclusionTimeout;
	}

	public Atom getAtom()
	{
		this.lock.readLock().lock();
		try
		{
			return this.atom;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
			
	void setAtom(Atom atom)
	{
		this.lock.writeLock().lock();
		try
		{
			if (Objects.requireNonNull(atom).getHash().equals(this.hash) == false)
				throw new IllegalArgumentException("Atom does not match hash "+this.hash+" "+atom);
	
			if (this.atom == null)
				this.atom = atom;
			else
				throw new IllegalStateException("Pending atom "+this.hash+" already has an atom object");
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	Collection<UInt256> getShards()
	{
		if (this.status.get().lessThan(CommitStatus.PREPARED) == true)
			throw new IllegalStateException("Pending atom "+this.getHash()+" is not PREPARED but "+this.status);
			
		return this.stateMachine.getShards();
	}
	
	Collection<StateKey<?, ?>> getStateKeys()
	{
		if (this.status.get().lessThan(CommitStatus.PREPARED) == true)
			throw new IllegalStateException("Pending atom "+this.getHash()+" is not PREPARED but "+this.status);
			
		return this.stateMachine.getStateKeys();
	}
	
	Collection<StateOp> getStateOps()
	{
		if (this.status.get().lessThan(CommitStatus.PREPARED) == true)
			throw new IllegalStateException("Pending atom "+this.getHash()+" is not PREPARED but "+this.status);
			
		return this.stateMachine.getStateOps();
	}
	
	void lock()
	{
		setStatus(CommitStatus.LOCKED);
	}

	void prepare() throws IOException, ValidationException
	{
		this.lock.writeLock().lock();
		try
		{
			if (this.atom == null)
				throw new IllegalStateException("Atom is null");
	
			if (this.stateMachine == null)
			{
				this.stateMachine = new StateMachine(this.context, this.atom);
				this.stateMachine.prepare();
				
				// FIXME needs to be a threshold per shard group for correctness.  A summed weight will suffice for testing.
				Set<Long> shardGroups = ShardMapper.toShardGroups(this.stateMachine.getShards(), this.context.getLedger().numShardGroups());
				this.voteThreshold = this.context.getLedger().getVoteRegulator().getVotePowerThreshold(this.context.getLedger().getHead().getHeight() - VoteRegulator.VOTE_POWER_MATURITY, shardGroups);
			}
	
			setStatus(CommitStatus.PREPARED);
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	Set<StateKey<?, ?>> provision(final BlockHeader block)
	{
		this.lock.writeLock().lock();
		try
		{
			Objects.requireNonNull(block, "Block is null");
		
			setStatus(CommitStatus.PROVISIONING);
			this.block = block.getHash();
			
			long commitTimeout = Longs.fromByteArray(block.getHash().toByteArray()) + PendingAtom.ATOM_COMMIT_TIMEOUT_BLOCKS;
			setCommitTimeout(commitTimeout);
			
			return this.stateMachine.provision(block);
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	synchronized boolean provision(StateKey<?, ?> key, UInt256 value) throws ValidationException
	{
		this.lock.writeLock().lock();
		try
		{
			if (this.stateMachine.thrown() != null)
			{
				cerbyLog.warn("Detected thrown exception for pending atom "+getHash()+" when provisioning "+key+" with "+value);
				return false;
			
			// FIXME want to throw this but currently causes issues as there are two call point for provision that are not 
			//		 aware of possible thrown provisioning exceptions before entering
//				throw new IllegalStateException("Detected thrown exception for pending atom "+getHash()+" when provisioning "+key+" with "+value);
			}
			
			if (this.status.get().equals(CommitStatus.PROVISIONING) == false)
				throw new IllegalStateException("Pending atom "+getHash()+" is not PROVISIONING but "+this.status);
			
			try
			{
				this.stateMachine.provision(key, value);
			}
			catch (Exception ex)
			{
				// TODO status setting is strict order so can't skip. but would like to skip here (or have a more relevant status)
				setStatus(CommitStatus.PROVISIONED);
				setStatus(CommitStatus.EXECUTED);
				throw ex;
			}

			if (this.stateMachine.isProvisioned() == true)
			{
				setStatus(CommitStatus.PROVISIONED);
				return true;
			}
			
			return false;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	void execute() throws ValidationException, IOException
	{
		this.lock.writeLock().lock();
		try
		{
			if (this.stateMachine.thrown() != null)
				throw new IllegalStateException("Detected thrown exception for pending atom "+getHash()+" when executing");

			if (this.status.get().equals(CommitStatus.PROVISIONED) == false) 
				throw new IllegalStateException("Pending atom "+this.getHash()+" is not PROVISIONED but "+this.status);
	
			try
			{
				this.stateMachine.execute();
			}
			finally
			{
				setStatus(CommitStatus.EXECUTED);
			}
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	Optional<UInt256> getInput(StateKey<?, ?> key)
	{
		this.lock.readLock().lock();
		try
		{
			return this.stateMachine.getInput(key);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	Optional<UInt256> getOutput(StateKey<?, ?> key)
	{
		this.lock.readLock().lock();
		try
		{
			return this.stateMachine.getOutput(key);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	Hash getExecution()
	{
		this.lock.readLock().lock();		
		try
		{
			return this.stateMachine.getExecution();
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public Exception thrown()
	{
		this.lock.readLock().lock();
		try
		{
			return this.stateMachine.thrown();
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}	
	
	CommitOperation getCommitOperation()
	{
		this.lock.writeLock().lock();
		try
		{
			if (this.certificate.getDecision().equals(StateDecision.POSITIVE) == true)
				return getCommitOperation(Type.ACCEPT);
			if (this.certificate.getDecision().equals(StateDecision.NEGATIVE) == true)
				return getCommitOperation(Type.REJECT);
			
			throw new UnsupportedOperationException("State decision "+this.certificate.getDecision()+" in certificate for "+this.getHash()+" not supported");
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	/** Gets the commit operation of the specified type.
	 * 
	 * WARNING: Calling this directly can have critical consequences! 
	 * 
	 * @param accept
	 * @return
	 */
	CommitOperation getCommitOperation(final Type type)
	{
		this.lock.writeLock().lock();
		try
		{
			if (type.equals(Type.ACCEPT) == true)
				return this.stateMachine.getAcceptOperation();
			if (type.equals(Type.REJECT) == true)
				return this.stateMachine.getRejectOperation();
			
			throw new UnsupportedOperationException("Commit operation type "+type+" for "+getHash()+" not supported");
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}


/*	void abort()
	{
		this.lock.writeLock().lock();
		try
		{
			setStatus(CommitStatus.ABORTED);
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}*/

	public CommitStatus getStatus()
	{
		return this.status.get();
	}
			
	void setStatus(final CommitStatus status)
	{
		Objects.requireNonNull(status, "Status is null");
		
		this.status.updateAndGet((s) -> 
		{
			// Allow multiple locks // TODO omit the locked status entirely?
			if (status.equals(CommitStatus.LOCKED) == false)
			{
				if (s.equals(status) == true)
					throw new IllegalStateException("Status of pending atom "+this.hash+" is already set to "+status);
				
				if (s.greaterThan(status) == true)
					throw new IllegalStateException("Pending atom "+this.hash+" has already been set to "+status+" now "+this.status);
	
				// Can always abort at any stage
				if (status.equals(CommitStatus.ABORTED) == false && s.index() < status.index()-1)
					throw new IllegalStateException("Pending atom "+this.hash+" can not skip to status "+status+" from "+this.status);
			}
			
			return status;
		});
	}

	public Hash getBlock()
	{
		this.lock.readLock().lock();
		try
		{
			return this.block;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
			
	long getInclusionDelay()
	{
		return this.inclusionDelay;
	}
			
	void setInclusionDelay(long delay)
	{
		this.inclusionDelay = delay;
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
		
		return true;
	}

	@Override
	public String toString()
	{
		return this.hash+" @ "+this.witnessed;
	}
	
	boolean voted(ECPublicKey identity)
	{
		this.lock.readLock().lock();
		try
		{
			return this.voted.containsKey(Objects.requireNonNull(identity, "Vote identity is null"));
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	Collection<AtomVote> votes()
	{
		this.lock.readLock().lock();
		try
		{
			return new ArrayList<AtomVote>(this.votes);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	Collection<AtomVote> queued()
	{
		this.lock.readLock().lock();
		try
		{
			return new ArrayList<AtomVote>(this.queued);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	void queue(final AtomVote vote)
	{
		this.lock.writeLock().lock();
		try
		{
			Objects.requireNonNull(vote, "Atom vote is null");
			this.queued.add(vote);
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	Collection<AtomVote> apply()
	{
		this.lock.writeLock().lock();
		try
		{
			if (getStatus().lessThan(CommitStatus.PREPARED) == true)
				return Collections.emptySet();
		
			List<AtomVote> applied = new ArrayList<AtomVote>();
			Iterator<AtomVote> queuedIterator = this.queued.iterator();
			while(queuedIterator.hasNext() == true)
			{
				AtomVote vote = queuedIterator.next();
				long votePower = this.context.getLedger().getVoteRegulator().getVotePower(this.context.getLedger().getHead().getHeight() - VoteRegulator.VOTE_POWER_MATURITY, vote.getOwner());
				if (this.voted.containsKey(vote.getOwner()) == false)
				{
					this.votes.add(vote);
					this.voted.put(vote.getOwner(), votePower);
					this.voteWeight += votePower;
					applied.add(vote);
				}
				else
					atomsLog.warn(this.context.getName()+": "+vote.getOwner()+" has already cast a vote for "+this.hash);
				
				queuedIterator.remove();
			}

			return applied;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	long voteWeight()
	{
		this.lock.readLock().lock();
		try
		{
			return this.voteWeight;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	/** 
	 * Returns the vote weight threshold needed for this atom to be selected for inclusion.
	 * <br>
	 * <br>
	 * Will not be set non-zero until after atom is PREPARED
	 * 
	 * @return minimum vote weight for inclusion
	 */
	public long voteThreshold() 
	{
		return this.voteThreshold;
	}
	
	public AtomCertificate getCertificate()
	{
		this.lock.readLock().lock();
		try
		{
			return this.certificate;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	public Collection<StateCertificate> getCertificates()
	{
		this.lock.readLock().lock();
		try
		{
			return new ArrayList<StateCertificate>(this.certificates.values());
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	boolean addCertificate(final StateCertificate certificate)
	{
		this.lock.writeLock().lock();
		try
		{
			if (this.certificates.containsKey(certificate.getState()) == false)
			{
				this.certificates.put(certificate.getState(), certificate);
				
				// Not in a block yet, effectively cancel the inclusion timeout
				if (this.block == null)
					setInclusionTimeout(Long.MAX_VALUE);
				else
				{
					// TODO The max timeout will be the highest block containing the atom in ANY of the shard groups
					// 		Not a perfect solution over the long terms as block heights in groups will potentially have a lot of disparity
					//		A mean timeout across all blocks of all shards is probably a better option
					long commitTimeout = Longs.fromByteArray(certificate.getBlock().toByteArray()) + PendingAtom.ATOM_COMMIT_TIMEOUT_BLOCKS;
					if (commitTimeout > this.commitTimeout)
						setCommitTimeout(commitTimeout);
				}
				
				return true;
			}
			
			return false;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	StateCertificate getCertificate(StateKey<?, ?> key)
	{
		this.lock.readLock().lock();
		try
		{
			return this.certificates.get(key);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	void setCertificate(AtomCertificate certificate)
	{
		this.lock.writeLock().lock();
		try
		{
			if (Objects.requireNonNull(certificate).getAtom().equals(this.hash) == false)
				throw new IllegalArgumentException("Atom certificate "+certificate.getHash()+" does not reference "+this.hash);
			
			this.certificate = certificate;
			setInclusionTimeout(Long.MAX_VALUE);
			setCommitTimeout(Long.MAX_VALUE);
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	AtomCertificate buildCertificate() throws CryptoException, ValidationException, IOException
	{
		this.lock.writeLock().lock();
		try
		{
			if (this.status.get().lessThan(CommitStatus.PROVISIONING) == true)
			{
				if (this.status.get().greaterThan(CommitStatus.LOCKED) == true)
					cerbyLog.warn(this.context.getName()+": Attempted to create atom certificate for "+getHash()+" when status "+this.status.get());
				
				return null;
			}
			
			if (getCertificate() != null)
			{
				cerbyLog.warn(this.context.getName()+": Atom certificate for "+getHash()+" already created");
				return this.certificate;
			}
			
			// TODO where does the validation of received certificates from other shard groups go? 
			//	    and what does it do?
			
			// Check possibility of creating an atom certificate.  
			// Either need a full complement of StateCertificates, or a single reject certificate from any shardgroup.
			boolean isComplete = true;
			boolean hasRejection = false;
			for (StateKey<?, ?> stateKey : this.stateMachine.getStateKeys())
			{
				if (this.certificates.containsKey(stateKey) == false)
				{
					isComplete = false;
					continue;
				}
				
				if (this.certificates.get(stateKey).getDecision().equals(StateDecision.NEGATIVE) == true)
					hasRejection = true;
			}
			
			if (isComplete == false && hasRejection == false)
				return null;
			
			for (StateCertificate certificate : this.certificates.values())
			{
				if (getHash().equals(certificate.getAtom()) == false)
				{
					// TODO handle this properly
					cerbyLog.error(this.context.getName()+": State certificate for "+certificate.getState()+" does not reference atom "+getHash());
					continue;
				}
			}
			
			Collection<VotePowerBloom> votePowerBlooms = new ArrayList<VotePowerBloom>();
			for (long shardGroup : ShardMapper.toShardGroups(this.stateMachine.getShards(), this.context.getLedger().numShardGroups()))
				votePowerBlooms.add(this.context.getLedger().getVoteRegulator().getVotePowerBloom(this.block, shardGroup));
			
			AtomCertificate certificate = new AtomCertificate(getHash(), this.certificates.values(), votePowerBlooms);
			setCertificate(certificate);
			this.context.getEvents().post(new AtomCertificateEvent(certificate));
			this.context.getMetaData().increment("ledger.pool.atom.certificates");
			cerbyLog.info(this.context.getName()+": Created atom certificate "+certificate.getHash()+" for atom "+getHash()+" with decision "+certificate.getDecision());
			
			return this.certificate;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
}