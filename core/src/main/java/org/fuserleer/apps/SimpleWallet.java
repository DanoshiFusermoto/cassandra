package org.fuserleer.apps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

import org.fuserleer.Context;
import org.fuserleer.common.Order;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.ECKeyPair;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.events.EventListener;
import org.fuserleer.ledger.AssociationSearchQuery;
import org.fuserleer.ledger.AssociationSearchResponse;
import org.fuserleer.ledger.AtomFuture;
import org.fuserleer.ledger.SearchResult;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.ledger.atoms.TokenSpecification;
import org.fuserleer.ledger.atoms.TransferParticle;
import org.fuserleer.ledger.atoms.Particle.Spin;
import org.fuserleer.ledger.events.AtomAcceptedEvent;
import org.fuserleer.ledger.events.AtomCommitTimeoutEvent;
import org.fuserleer.ledger.events.AtomExceptionEvent;
import org.fuserleer.ledger.events.AtomRejectedEvent;
import org.fuserleer.ledger.exceptions.InsufficientBalanceException;
import org.fuserleer.utils.UInt256;

import com.google.common.eventbus.Subscribe;

public class SimpleWallet implements AutoCloseable
{
	private final Context context;
	private final ECKeyPair key;
	private final Set<TransferParticle> unconsumed = Collections.synchronizedSet(new LinkedHashSet<TransferParticle>());
	private final Map<Hash, AtomFuture> futures = Collections.synchronizedMap(new HashMap<Hash, AtomFuture>()); 
	
	public SimpleWallet(final Context context, final ECKeyPair key) throws IOException, InterruptedException, ExecutionException
	{
		this.context = Objects.requireNonNull(context);
		this.key = Objects.requireNonNull(key);
		
		long searchOffset = 0;
		AssociationSearchQuery search = new AssociationSearchQuery(key.getPublicKey().asHash(), TransferParticle.class, Order.ASCENDING, 25);
		Future<AssociationSearchResponse> searchResponseFuture;
		
		while((searchResponseFuture = this.context.getLedger().get(search, Spin.UP)).get().isEmpty() == false)
		{
			for (SearchResult searchResult : searchResponseFuture.get().getResults())
				this.unconsumed.add(searchResult.getPrimitive());
			
			searchOffset = searchResponseFuture.get().getQuery().getOffset();
			if (searchOffset == -1)
				break;
			
			search = new AssociationSearchQuery(key.getPublicKey().asHash(), Atom.class, Order.ASCENDING, searchOffset, 25);
		}
		
		this.context.getEvents().register(this.atomListener);
	}
	
	public void close()
	{
		this.context.getEvents().unregister(this.atomListener);
	}
	
	public ECPublicKey getIdentity()
	{
		return this.key.getPublicKey();
	}

	public UInt256 getBalance(final TokenSpecification token)
	{
		return getBalance(token.getHash());
	}

	public UInt256 getBalance(final Hash token)
	{
		Objects.requireNonNull(token);
		
		UInt256 balance = UInt256.ZERO;
		synchronized(this.unconsumed)
		{
			List<TransferParticle> particles = this.unconsumed.stream().filter(tp -> tp.getToken().equals(token)).collect(Collectors.toList());
			for (TransferParticle particle : particles)
				balance = balance.add(particle.getQuantity());
		}
		
		return balance;
	}
	
	public void inject(Collection<TransferParticle> particles)
	{
		particles.forEach(tp -> 
		{
			if (tp.getOwner().equals(SimpleWallet.this.key.getPublicKey()) == false)
				return;
			
			if (tp.getSpin().equals(Spin.UP) == true)
				SimpleWallet.this.unconsumed.add(tp);

			if (tp.getSpin().equals(Spin.DOWN) == true)
				SimpleWallet.this.unconsumed.remove(tp.get(Spin.UP));
		});
	}
	
	public Atom getSplitAtom(final TokenSpecification token, int slices) throws CryptoException, InsufficientBalanceException
	{
		return getSplitAtom(token.getHash(), slices);
	}

	public Atom getSplitAtom(final Hash token, int slices) throws CryptoException, InsufficientBalanceException
	{
		return new Atom(getSplitParticles(token, slices));
	}
	
	public Collection<TransferParticle> getSplitParticles(final Hash token, int slices) throws CryptoException, InsufficientBalanceException
	{
		UInt256 balance = getBalance(token);
		if (balance.compareTo(UInt256.from(slices)) < 0)
			throw new InsufficientBalanceException(this.key.getPublicKey(), token, UInt256.from(slices), this.key.getPublicKey());
		
		List<TransferParticle> selected = new ArrayList<TransferParticle>();
		UInt256 credit = UInt256.ZERO;
		synchronized(this.unconsumed)
		{
			for (TransferParticle particle : this.unconsumed)
			{
				if (particle.getToken().equals(token) == true)
				{
					selected.add(particle);
					credit = credit.add(particle.getQuantity());
				}
			}
			
			List<TransferParticle> particles = new ArrayList<TransferParticle>();
			for (TransferParticle particle : selected)
			{
				particle = particle.get(Spin.DOWN);
				particle.sign(this.key);
				particles.add(particle);
			}
		
			UInt256 sliceQuantity = credit.divide(UInt256.from(slices));
			for (int s = 0 ; s < slices ; s++)
			{
				TransferParticle transfer = new TransferParticle(sliceQuantity, token, Spin.UP, this.key.getPublicKey()); 
				particles.add(transfer);
				credit = credit.subtract(sliceQuantity);
			}
		
			if (credit.compareTo(UInt256.ZERO) > 0)
			{
				TransferParticle change = new TransferParticle(credit, token, Spin.UP, this.key.getPublicKey()); 
				particles.add(change);
			}
		
			return particles;
		}
	}

	public Atom getSpendAtom(final TokenSpecification token, final UInt256 quantity, final ECPublicKey to) throws CryptoException, InsufficientBalanceException
	{
		return getSpendAtom(token.getHash(), quantity, to);
	}

	public Atom getSpendAtom(final Hash token, final UInt256 quantity, final ECPublicKey to) throws CryptoException, InsufficientBalanceException
	{
		return new Atom(getSpendParticles(token, quantity, to));
	}
	
	public Collection<TransferParticle> getSpendParticles(final Hash token, final UInt256 quantity, final ECPublicKey to) throws CryptoException, InsufficientBalanceException
	{
		UInt256 balance = getBalance(token);
		if (balance.compareTo(quantity) < 0)
			throw new InsufficientBalanceException(this.key.getPublicKey(), token, quantity, to);
		
		List<TransferParticle> selected = new ArrayList<TransferParticle>();
		UInt256 credit = UInt256.ZERO;
		synchronized(this.unconsumed)
		{
			for (TransferParticle particle : this.unconsumed)
			{
				if (particle.getToken().equals(token) == true)
				{
					selected.add(particle);
					credit = credit.add(particle.getQuantity());
					
					if (credit.compareTo(quantity) >= 0)
						break;
				}
			}
			
			List<TransferParticle> particles = new ArrayList<TransferParticle>();
			for (TransferParticle particle : selected)
			{
				particle = particle.get(Spin.DOWN);
				particle.sign(this.key);
				particles.add(particle);
			}
		
			TransferParticle transfer = new TransferParticle(quantity, token, Spin.UP, to); 
			particles.add(transfer);
		
			if (credit.subtract(quantity).compareTo(UInt256.ZERO) > 0)
			{
				TransferParticle change = new TransferParticle(credit.subtract(quantity), token, Spin.UP, this.key.getPublicKey()); 
				particles.add(change);
			}
		
			return particles;
		}
	}
	
	public Future<AtomCertificate> submit(Atom atom) throws InterruptedException
	{
		AtomFuture atomFuture;
		synchronized(this.futures)
		{
			atomFuture = this.futures.get(atom.getHash());
			if (atomFuture == null)
			{
				atomFuture = new AtomFuture(atom);
				this.futures.put(atom.getHash(), atomFuture);
				
				if (SimpleWallet.this.context.getLedger().submit(atom) == false)
				{
					atomFuture.completeExceptionally(new RejectedExecutionException("Submission of atom "+atom.getHash()+" failed"));
					this.futures.remove(atom.getHash());
					return atomFuture;
				}
			}
			else
				return atomFuture;
		}

		synchronized(this.unconsumed)
		{
			atom.getParticles(TransferParticle.class).forEach(tp -> 
			{
				if (tp.getOwner().equals(SimpleWallet.this.key.getPublicKey()) == false)
					return;
				
				if (tp.getSpin().equals(Spin.DOWN) == true)
					SimpleWallet.this.unconsumed.remove(tp.get(Spin.UP));
			});
			
			return atomFuture;
		}
	}

	// ACTION LISTENER //
	private EventListener atomListener = new EventListener() 
	{
		@Subscribe
		public void on(final AtomAcceptedEvent event) 
		{
			SimpleWallet.this.futures.remove(event.getAtom().getHash(), event.getAtom());
			event.getAtom().getParticles(TransferParticle.class).forEach(tp -> 
			{
				if (tp.getOwner().equals(SimpleWallet.this.key.getPublicKey()) == false)
					return;
				
				if (tp.getSpin().equals(Spin.UP) == true)
					SimpleWallet.this.unconsumed.add(tp);

				if (tp.getSpin().equals(Spin.DOWN) == true)
					SimpleWallet.this.unconsumed.remove(tp.get(Spin.UP));
			});
		}
		
		@Subscribe
		public void on(final AtomRejectedEvent event) 
		{
			SimpleWallet.this.futures.remove(event.getAtom().getHash(), event.getAtom());
			event.getAtom().getParticles(TransferParticle.class).forEach(tp -> 
			{
				if (tp.getOwner().equals(SimpleWallet.this.key.getPublicKey()) == false)
					return;
				
				if (tp.getSpin().equals(Spin.UP) == true)
					SimpleWallet.this.unconsumed.remove(tp);
				
				if (tp.getSpin().equals(Spin.DOWN) == true)
					SimpleWallet.this.unconsumed.add(tp.get(Spin.UP));
			});
		}
		
		@Subscribe
		public void on(final AtomCommitTimeoutEvent event) 
		{
			SimpleWallet.this.futures.remove(event.getAtom().getHash(), event.getAtom());
			event.getAtom().getParticles(TransferParticle.class).forEach(tp -> 
			{
				if (tp.getOwner().equals(SimpleWallet.this.key.getPublicKey()) == false)
					return;
				
				if (tp.getSpin().equals(Spin.UP) == true)
					SimpleWallet.this.unconsumed.remove(tp);
				
				if (tp.getSpin().equals(Spin.DOWN) == true)
					SimpleWallet.this.unconsumed.add(tp.get(Spin.UP));
			});
		}

		@Subscribe
		public void on(final AtomExceptionEvent event) 
		{
			SimpleWallet.this.futures.remove(event.getAtom().getHash(), event.getAtom());
			event.getAtom().getParticles(TransferParticle.class).forEach(tp -> 
			{
				if (tp.getOwner().equals(SimpleWallet.this.key.getPublicKey()) == false)
					return;
				
				if (tp.getSpin().equals(Spin.UP) == true)
					SimpleWallet.this.unconsumed.remove(tp);
				
				if (tp.getSpin().equals(Spin.DOWN) == true)
					SimpleWallet.this.unconsumed.add(tp.get(Spin.UP));
			});
		}
	};
}
