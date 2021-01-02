package org.fuserleer.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.Hash;
import org.fuserleer.database.Indexable;
import org.fuserleer.events.EventListener;
import org.fuserleer.events.SynchronousEventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.executors.Executable;
import org.fuserleer.executors.Executor;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.ledger.events.AtomCommitTimeoutEvent;
import org.fuserleer.ledger.events.AtomCommittedEvent;
import org.fuserleer.ledger.events.BlockCommittedEvent;
import org.fuserleer.ledger.events.StateCertificateEvent;
import org.fuserleer.ledger.messages.StateCertificateMessage;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.messaging.MessageProcessor;
import org.fuserleer.network.peers.ConnectedPeer;

import com.google.common.eventbus.Subscribe;

public final class StateHandler implements Service
{
	private static final Logger cerbyLog = Logging.getLogger("cerby");

	public final static int ATOM_COMMIT_TIMEOUT = 10;
	
	private final Context context;
	
	private class PendingAtom
	{
		// TODO needs timeout
		private final Hash	hash;
		private final long	seen;
		private	Atom		atom;
		private AtomCertificate certificate;
		private final Map<Hash, StateCertificate> certificates;

		public PendingAtom(final Hash atom, final long seen)
		{
			if (seen < 0)
				throw new IllegalArgumentException("Seen block height is negative");

			this.seen = seen;
			this.hash = Objects.requireNonNull(atom);
			this.certificates = Collections.synchronizedMap(new HashMap<Hash, StateCertificate>());
		}

		public Hash getHash()
		{
			return this.hash;
		}
		
		public Atom getAtom()
		{
			return this.atom;
		}
		
		public long getSeen()
		{
			return this.seen;
		}

		public AtomCertificate getCertificate()
		{
			return this.certificate;
		}

		void setAtom(Atom atom)
		{
			if (Objects.requireNonNull(atom).getHash().equals(this.hash) == false)
				throw new IllegalArgumentException("Atom "+atom.getHash()+" does not match hash "+this.hash);
			
			this.atom = atom;
		}

		void setCertificate(AtomCertificate certificate)
		{
			if (Objects.requireNonNull(certificate).getAtom().equals(this.hash) == false)
				throw new IllegalArgumentException("Certificate "+certificate.getHash()+" does not reference Atom "+this.hash);
			
			this.certificate = certificate;
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
			return this.hash.toString();
		}
		
		public void add(final StateCertificate certificate)
		{
			this.certificates.putIfAbsent(certificate.getState(), certificate);
		}
	}
	
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private final Map<Hash, PendingAtom> pending = new HashMap<Hash, PendingAtom>();

	StateHandler(Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
	}
	
	@Override
	public void start() throws StartupException
	{
		this.context.getNetwork().getMessaging().register(StateCertificateMessage.class, this.getClass(), new MessageProcessor<StateCertificateMessage>()
		{
			@Override
			public void process(final StateCertificateMessage stateCertificateMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							if (cerbyLog.hasLevel(Logging.DEBUG) == true)
								cerbyLog.debug(StateHandler.this.context.getName()+": State certificate "+stateCertificateMessage.getCertificate().getState()+" from " + peer);
							
							processStateCertificate(stateCertificateMessage.getCertificate());
						}
						catch (Exception ex)
						{
							cerbyLog.error(StateHandler.this.context.getName()+": ledger.messages.state.certificate " + peer, ex);
						}
					}
				});
			}
		});

		
		this.context.getEvents().register(this.syncBlockListener);
		this.context.getEvents().register(this.syncAtomListener);
		this.context.getEvents().register(this.stateCertificateListener);
	}

	@Override
	public void stop() throws TerminationException
	{
		this.context.getEvents().unregister(this.stateCertificateListener);
		this.context.getEvents().unregister(this.syncAtomListener);
		this.context.getEvents().unregister(this.syncBlockListener);
		
		this.context.getNetwork().getMessaging().deregisterAll(this.getClass());
	}

	private void buildCertificate(PendingAtom pendingAtom) throws CryptoException, ValidationException, IOException
	{
		Objects.requireNonNull(pendingAtom);
		if (pendingAtom.getCertificate() != null || pendingAtom.getAtom() == null)
			return;

		this.lock.writeLock().lock();
		try
		{
			
			// TODO where does the validation of received certificates go? and what does it do?
			for (Hash state : pendingAtom.getAtom().getStates())
			{
				if (pendingAtom.certificates.containsKey(state) == false)
					return;
			}

			AtomCertificate certificate = new AtomCertificate(pendingAtom.getHash(), pendingAtom.certificates.values());
			pendingAtom.setCertificate(certificate);
			this.context.getLedger().getLedgerStore().store(certificate);
			cerbyLog.info(StateHandler.this.context.getName()+": Created certificate "+certificate.getHash()+" for atom "+pendingAtom.getHash()+" with decision "+certificate.getDecision());
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	public List<AtomCertificate> get(final int limit, final Collection<Hash> exclusions)
	{
		final List<AtomCertificate> certificates = new ArrayList<AtomCertificate>();
		final Predicate<PendingAtom> filter = new Predicate<PendingAtom>()
		{
			@Override
			public boolean test(PendingAtom pa)
			{
				if (pa.atom == null)
					return false;

				if (pa.certificate == null)
					return false;

				if (exclusions.contains(pa.certificate.getHash()) == true)
					return false;
					
				return true;
			}
		};

		this.lock.readLock().lock();
		try
		{
			for (PendingAtom pendingAtom : this.pending.values())
			{
				if (filter.test(pendingAtom) == false)
					continue;
				
				certificates.add(pendingAtom.getCertificate());
				
				if (certificates.size() == limit)
					break;
			}
		}
		finally
		{
			this.lock.readLock().unlock();
		}
		
		return certificates;
	}
	
	private void processStateCertificate(StateCertificate certificate)
	{
		// Creating pending atom from certificate if not seen // It should probably have been seen locally via a block committed event at this point, verify that is the case
		StateHandler.this.lock.writeLock().lock();
		try
		{
			PendingAtom pendingAtom = StateHandler.this.pending.get(certificate.getAtom());
			CommitState commitState = StateHandler.this.context.getLedger().getStateAccumulator().has(Indexable.from(certificate.getAtom(), AtomCertificate.class));
			if (pendingAtom == null && commitState.index() < CommitState.COMMITTED.index())
			{
				cerbyLog.warn(StateHandler.this.context.getName()+": Heard about Atom "+certificate.getAtom()+" via StateCertificateEvent before BlockCommittedEvent");
				pendingAtom = new PendingAtom(certificate.getAtom(), StateHandler.this.context.getLedger().getHead().getHeight());
				StateHandler.this.pending.put(certificate.getAtom(), pendingAtom);
			}
			
			if (commitState.index() == CommitState.COMMITTED.index())
			{
				cerbyLog.warn(StateHandler.this.context.getName()+": Already have a certificate for "+certificate.getAtom());
				StateHandler.this.pending.remove(certificate.getAtom());
				return;
			}

			if (pendingAtom != null)
			{
				pendingAtom.add(certificate);
				buildCertificate(pendingAtom);
			}
		}
		catch (Exception ex)
		{
			cerbyLog.error(StateHandler.class.getName()+": Failed to process state certificate for PendingAtom "+certificate.getAtom()+" on state "+certificate.getState(), ex);
			return;
		}
		finally
		{
			StateHandler.this.lock.writeLock().unlock();
		}
	}
	
	
	// PARTICLE CERTIFICATE LISTENER //
	private EventListener stateCertificateListener = new EventListener()
	{
		@Subscribe
		public void on(final StateCertificateEvent stateCertificateEvent) 
		{
			if (cerbyLog.hasLevel(Logging.DEBUG) == true)
				cerbyLog.debug(StateHandler.this.context.getName()+": State certificate "+stateCertificateEvent.getCertificate().getState()+" from local");

			StateHandler.this.processStateCertificate(stateCertificateEvent.getCertificate());
		}
	};
		
	// SYNC ATOM LISTENER //
	private SynchronousEventListener syncAtomListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final AtomCommittedEvent atomCommittedEvent) 
		{
			StateHandler.this.lock.writeLock().lock();
			try
			{
				StateHandler.this.pending.remove(atomCommittedEvent.getAtom().getHash());
			}
			finally
			{
				StateHandler.this.lock.writeLock().unlock();
			}
		}
	};

	// SYNC BLOCK LISTENER //
	private SynchronousEventListener syncBlockListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final BlockCommittedEvent blockCommittedEvent) 
		{
			// Creating pending atom from precommitted event if not seen // This is the most likely place for a pending atom object to be created
			StateHandler.this.lock.writeLock().lock();
			try
			{
				try
				{
					for (Atom atom : blockCommittedEvent.getBlock().getAtoms())
					{
						CommitState commitState = StateHandler.this.context.getLedger().getStateAccumulator().has(Indexable.from(atom.getHash(), AtomCertificate.class));
						if (commitState.index() == CommitState.COMMITTED.index())
						{
							cerbyLog.warn(StateHandler.this.context.getName()+": Already have a certificate for "+atom.getHash());
							StateHandler.this.pending.remove(atom.getHash());
							continue;
						}
	
						PendingAtom pendingAtom = StateHandler.this.pending.get(atom.getHash());
						if (pendingAtom == null)
						{
							pendingAtom = new PendingAtom(atom.getHash(), blockCommittedEvent.getBlock().getHeader().getHeight());
							StateHandler.this.pending.put(atom.getHash(), pendingAtom);
						}
						
						if (pendingAtom != null)
							pendingAtom.setAtom(atom);
					}
				}
				catch (Exception ex)
				{
					cerbyLog.error(StateHandler.class.getName()+": Failed to create PendingAtom set for "+blockCommittedEvent.getBlock().getHeader()+" when processing BlockCommittedEvent", ex);
					return;
				}

				try
				{
					// Timeouts
					Set<PendingAtom> timedOut = new HashSet<PendingAtom>();
					for (PendingAtom pendingAtom : StateHandler.this.pending.values())
					{
						if (blockCommittedEvent.getBlock().getHeader().getHeight() - pendingAtom.getSeen() >= StateHandler.ATOM_COMMIT_TIMEOUT)
							timedOut.add(pendingAtom);
					}
					
					for (PendingAtom pendingAtom : timedOut)
					{
						if (pendingAtom.getAtom() == null)
							cerbyLog.warn(StateHandler.this.context.getName()+": Atom "+pendingAtom.getHash()+" timeout but never seen at "+blockCommittedEvent.getBlock().getHeader());
						else
						{
							cerbyLog.warn(StateHandler.this.context.getName()+": Atom "+pendingAtom.getHash()+" timeout at block "+blockCommittedEvent.getBlock().getHeader());
							StateHandler.this.pending.remove(pendingAtom.getHash());
							StateHandler.this.context.getEvents().post(new AtomCommitTimeoutEvent(pendingAtom.getAtom()));
						}
					}
				}
				catch (Exception ex)
				{
					cerbyLog.error(StateHandler.class.getName()+": Processing of atom commit timeouts failed", ex);
					return;
				}
			}
			finally
			{
				StateHandler.this.lock.writeLock().unlock();
			}
		}
	};
}
