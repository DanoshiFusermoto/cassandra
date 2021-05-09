package org.fuserleer.apps;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.fuserleer.Context;
import org.fuserleer.common.Direction;
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
import org.fuserleer.ledger.ShardMapper;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.ledger.atoms.MessageParticle;
import org.fuserleer.ledger.atoms.Particle;
import org.fuserleer.ledger.atoms.TokenSpecification;
import org.fuserleer.ledger.atoms.TokenParticle;
import org.fuserleer.ledger.atoms.TokenParticle.Action;
import org.fuserleer.ledger.atoms.Particle.Spin;
import org.fuserleer.ledger.atoms.SignedParticle;
import org.fuserleer.ledger.events.AtomAcceptedEvent;
import org.fuserleer.ledger.events.AtomAcceptedTimeoutEvent;
import org.fuserleer.ledger.events.AtomCommitTimeoutEvent;
import org.fuserleer.ledger.events.AtomExceptionEvent;
import org.fuserleer.ledger.events.AtomRejectedEvent;
import org.fuserleer.ledger.exceptions.InsufficientBalanceException;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.Protocol;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.network.peers.PeerState;
import org.fuserleer.network.peers.filters.StandardPeerFilter;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.utils.UInt256;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.json.JSONObject;

import com.google.common.eventbus.Subscribe;

/** A simple wallet for interacting with the network.  Not very efficient and not very good, but adequate for most uses / testing.
 * 
 * @author Dan
 *
 */
public class SimpleWallet implements AutoCloseable
{
	private static final Logger apiLog = Logging.getLogger("api");
	private static final Logger walletLog = Logging.getLogger("wallet");

	private final Context context;
	private final ECKeyPair key;
	private final Map<Hash, Particle> particles = Collections.synchronizedMap(new LinkedHashMap<Hash, Particle>());
	private final Set<TokenParticle> unconsumed = Collections.synchronizedSet(new LinkedHashSet<TokenParticle>());
	private final Map<Hash, AtomFuture> futures = Collections.synchronizedMap(new HashMap<Hash, AtomFuture>()); 
	private final Map<ConnectedPeer, WebSocketClient> websockets = new HashMap<ConnectedPeer, WebSocketClient>();
	
	private transient boolean closed = false;
	
	private ReentrantLock lock = new ReentrantLock(true);
	
	public SimpleWallet(final Context context, final ECKeyPair key) throws IOException, InterruptedException, ExecutionException, URISyntaxException
	{
		this.context = Objects.requireNonNull(context);
		this.key = Objects.requireNonNull(key);
		
		long searchOffset = 0;
		AssociationSearchQuery search = new AssociationSearchQuery(key.getPublicKey().asHash(), Particle.class, Order.ASCENDING, 25);
		Future<AssociationSearchResponse> searchResponseFuture;
		
		while((searchResponseFuture = this.context.getLedger().get(search, Spin.UP)).get().isEmpty() == false)
		{
			for (SearchResult searchResult : searchResponseFuture.get().getResults())
			{
				this.particles.put(searchResult.getType().cast(searchResult.getPrimitive()).getHash(), searchResult.getPrimitive());
				
				if (searchResult.getPrimitive() instanceof TokenParticle)
				{
					TokenParticle transferParticle = ((TokenParticle)searchResult.getPrimitive());
					if (transferParticle.getSpin().equals(Spin.UP) == true && transferParticle.getAction().equals(Action.TRANSFER) == true)
						SimpleWallet.this.unconsumed.add(transferParticle);
		
					if (transferParticle.getSpin().equals(Spin.DOWN) == true && transferParticle.getAction().equals(Action.TRANSFER) == true)
						SimpleWallet.this.unconsumed.remove(transferParticle.get(Spin.UP));
				}
			}
			
			searchOffset = searchResponseFuture.get().getNextOffset();
			if (searchOffset == -1)
				break;
			
			search = new AssociationSearchQuery(key.getPublicKey().asHash(), Particle.class, Order.ASCENDING, searchOffset, 25);
		}
		
//		this.context.getEvents().register(this.atomListener);
		openWebSockets();
	}
	
	public void close()
	{
		this.lock.lock();
		try
		{
			this.closed = true;
			this.websockets.forEach((p, s) -> s.close());
	//		this.context.getEvents().unregister(this.atomListener);
			this.websockets.clear();
			this.particles.clear();
			this.unconsumed.clear();
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	private void openWebSockets() throws URISyntaxException, InterruptedException
	{
		// Set up basic websocket clients
		for (long shardGroup = 0 ; shardGroup < this.context.getLedger().numShardGroups(context.getLedger().getHead().getHeight()) ; shardGroup++)
		{
			List<ConnectedPeer> shardConnected = this.context.getNetwork().get(StandardPeerFilter.build(this.context).setProtocol(Protocol.TCP).setStates(PeerState.CONNECTING, PeerState.CONNECTED).setShardGroup(shardGroup));
			for (ConnectedPeer peer : shardConnected)
			{
				if (openWebsocket(peer) != null)
					break;
			}
		}
	}
	
	private WebSocketClient openWebsocket(final ConnectedPeer peer) throws InterruptedException, URISyntaxException
	{
		WebSocketClient websocketConnection = new WebSocketClient(new URI("ws://"+peer.getURI().getHost()+":"+peer.getNode().getWebsocketPort())) 
		{
			ConnectedPeer connectedPeer;
			
			@Override
			public void onOpen(ServerHandshake handshakedata)
			{
				this.connectedPeer = peer;
				SimpleWallet.this.websockets.put(connectedPeer, this);
			}

			@Override
			public void onMessage(String message)
			{
				JSONObject messageJSON = new JSONObject(message);
				
				List<Particle> particles = new ArrayList<Particle>();
				for(int p = 0 ; p < messageJSON.getJSONArray("particles").length() ; p++)
					particles.add(Serialization.getInstance().fromJsonObject(messageJSON.getJSONArray("particles").getJSONObject(p), Particle.class));
				
				Atom atom = new Atom(particles);
				if (messageJSON.getString("atom").compareToIgnoreCase(atom.getHash().toString()) != 0)
				{
					apiLog.error(SimpleWallet.this.context.getName()+": Reconstructed atom hash is "+atom.getHash()+" but expected "+messageJSON.getString("atom"));
					return;
				}
				
				AtomCertificate certificate = null;
				if (messageJSON.has("certificate") == true)
					certificate = Serialization.getInstance().fromJsonObject(messageJSON.getJSONObject("certificate"), AtomCertificate.class);
					
				String type = messageJSON.getString("type");
				if (type.equalsIgnoreCase("accepted") == true)
					SimpleWallet.this.accept(atom, certificate);
				else if (type.equalsIgnoreCase("rejected") == true)
					SimpleWallet.this.reject(atom, certificate);
				else if (type.equalsIgnoreCase("timeout") == true)
					SimpleWallet.this.timeout(atom);
				else if (type.equalsIgnoreCase("exception") == true)
					SimpleWallet.this.exception(atom, new Exception(messageJSON.getString("error")));
				else
					walletLog.error(SimpleWallet.this.context.getName()+": Unknown web socket message type "+type+" -> "+messageJSON.toString());
			}

			@Override
			public void onClose(int code, String reason, boolean remote)
			{
				if (SimpleWallet.this.websockets.remove(this.connectedPeer, this) == false)
					walletLog.error(SimpleWallet.this.context.getName()+": Connection not found "+this.connectedPeer);
					
				if (SimpleWallet.this.closed == true)
					return;
				
				boolean reconnected = false;
				long numShardGroups = SimpleWallet.this.context.getLedger().numShardGroups();
				long shardGroup = ShardMapper.toShardGroup(this.connectedPeer.getNode().getIdentity(), numShardGroups);
				List<ConnectedPeer> shardConnected = SimpleWallet.this.context.getNetwork().get(StandardPeerFilter.build(SimpleWallet.this.context).setProtocol(Protocol.TCP).setStates(PeerState.CONNECTING, PeerState.CONNECTED).setDirection(Direction.OUTBOUND).setShardGroup(shardGroup));
				for (ConnectedPeer peer : shardConnected)
				{
					try
					{
						if (openWebsocket(peer) != null)
						{
							reconnected = true;
							break;
						}
					}
					catch (Exception ex)
					{
						walletLog.error(SimpleWallet.this.context.getName()+": Reconnection attempt to "+peer+" failed", ex);
					}
				}
				
				if (reconnected == false)
					walletLog.fatal(SimpleWallet.this.context.getName()+": Reconnection attempts shard group "+shardGroup+" failed, closing wallet");
			}

			@Override
			public void onError(Exception ex)
			{
				// TODO Auto-generated method stub
				
			}
		};
		
		if (websocketConnection.connectBlocking(5, TimeUnit.SECONDS) == false)
		{
			walletLog.error(this.context.getName()+": Failed to open web socket to "+websocketConnection.getURI());
			return null;
		}
		
		walletLog.info(this.context.getName()+": Connected to web socket at "+websocketConnection.getURI());
		return websocketConnection;
	}
	
	@SuppressWarnings("unchecked")
	public <T extends Particle> Collection<T> get(final Class<T> type, final Spin spin)
	{
		this.lock.lock();
		try
		{
			List<T> particles = new ArrayList<T>();
			for (Particle particle : this.particles.values())
			{
				if(spin.equals(Spin.ANY) == false && particle.getSpin().equals(spin) == false)
					continue;
				
				if (type.isAssignableFrom(particle.getClass()) == false)
					continue;
				
				particles.add((T) particle);
			}
			return particles;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	public ECPublicKey getIdentity()
	{
		return this.key.getPublicKey();
	}

	public void sign(SignedParticle particle) throws CryptoException
	{
		particle.sign(this.key);
	}

	public UInt256 getBalance(final TokenSpecification token)
	{
		return getBalance(token.getHash());
	}

	public UInt256 getBalance(final Hash token)
	{
		Objects.requireNonNull(token);
		
		UInt256 balance = UInt256.ZERO;
		this.lock.lock();
		try
		{
			List<TokenParticle> particles = this.unconsumed.stream().filter(tp -> tp.getToken().equals(token)).collect(Collectors.toList());
			for (TokenParticle particle : particles)
				if (particle.getAction().equals(Action.TRANSFER))
					balance = balance.add(particle.getQuantity());
		}
		finally
		{
			this.lock.unlock();
		}
		
		return balance;
	}
	
	public void inject(Collection<TokenParticle> particles)
	{
		this.lock.lock();
		try
		{
			particles.forEach(tp -> 
			{
				if (tp.getOwner().equals(SimpleWallet.this.key.getPublicKey()) == false)
					return;
				
				SimpleWallet.this.particles.put(tp.getHash(), tp);
				
				if (tp.getSpin().equals(Spin.UP) == true && tp.getAction().equals(Action.TRANSFER) == true)
					SimpleWallet.this.unconsumed.add(tp);
	
				if (tp.getSpin().equals(Spin.DOWN) == true && tp.getAction().equals(Action.TRANSFER) == true)
					SimpleWallet.this.unconsumed.remove(tp.get(Spin.UP));
			});
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	public Atom split(final TokenSpecification token, int slices) throws CryptoException, InsufficientBalanceException
	{
		return split(token.getHash(), slices);
	}

	public Atom split(final Hash token, int slices) throws CryptoException, InsufficientBalanceException
	{
		return new Atom(splitParticles(token, slices));
	}
	
	private Collection<TokenParticle> splitParticles(final Hash token, int slices) throws CryptoException, InsufficientBalanceException
	{
	
		this.lock.lock();
		try
		{
			UInt256 balance = getBalance(token);
			if (balance.compareTo(UInt256.from(slices)) < 0)
				throw new InsufficientBalanceException(this.key.getPublicKey(), token, UInt256.from(slices), this.key.getPublicKey());
			
			List<TokenParticle> selected = new ArrayList<TokenParticle>();
			UInt256 credit = UInt256.ZERO;
			for (TokenParticle particle : this.unconsumed)
			{
				if (particle.getToken().equals(token) == true && particle.getAction().equals(Action.TRANSFER) == true)
				{
					selected.add(particle);
					credit = credit.add(particle.getQuantity());
				}
			}
				
			List<TokenParticle> particles = new ArrayList<TokenParticle>();
			for (TokenParticle particle : selected)
			{
				particle = particle.get(Spin.DOWN);
				particle.sign(this.key);
				particles.add(particle);
			}
		
			UInt256 sliceQuantity = credit.divide(UInt256.from(slices));
			for (int s = 0 ; s < slices ; s++)
			{
				TokenParticle transfer = new TokenParticle(sliceQuantity, token, Action.TRANSFER, Spin.UP, this.key.getPublicKey()); 
				particles.add(transfer);
				credit = credit.subtract(sliceQuantity);
			}
		
			if (credit.compareTo(UInt256.ZERO) > 0)
			{
				TokenParticle change = new TokenParticle(credit, token, Action.TRANSFER, Spin.UP, this.key.getPublicKey()); 
				particles.add(change);
			}
		
			return particles;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	public Atom spend(final TokenSpecification token, final UInt256 quantity, final ECPublicKey to) throws CryptoException, InsufficientBalanceException
	{
		return spend(token.getHash(), quantity, to);
	}

	public Atom spend(final Hash token, final UInt256 quantity, final ECPublicKey to) throws CryptoException, InsufficientBalanceException
	{
		return new Atom(spendParticles(token, quantity, to));
	}
	
	private Collection<TokenParticle> spendParticles(final Hash token, final UInt256 quantity, final ECPublicKey to) throws CryptoException, InsufficientBalanceException
	{
		this.lock.lock();
		try
		{
			UInt256 balance = getBalance(token);
			if (balance.compareTo(quantity) < 0)
				throw new InsufficientBalanceException(this.key.getPublicKey(), token, quantity, to);
			
			List<TokenParticle> selected = new ArrayList<TokenParticle>();
			UInt256 credit = UInt256.ZERO;
			for (TokenParticle particle : this.unconsumed)
			{
				if (particle.getToken().equals(token) == true && particle.getAction().equals(Action.TRANSFER) == true)
				{
					selected.add(particle);
					credit = credit.add(particle.getQuantity());
					
					if (credit.compareTo(quantity) >= 0)
						break;
				}
			}
			
			List<TokenParticle> particles = new ArrayList<TokenParticle>();
			for (TokenParticle particle : selected)
			{
				particle = particle.get(Spin.DOWN);
				particle.sign(this.key);
				particles.add(particle);
			}
		
			TokenParticle transfer = new TokenParticle(quantity, token, Action.TRANSFER, Spin.UP, to); 
			particles.add(transfer);
		
			if (credit.subtract(quantity).compareTo(UInt256.ZERO) > 0)
			{
				TokenParticle change = new TokenParticle(credit.subtract(quantity), token, Action.TRANSFER, Spin.UP, this.key.getPublicKey()); 
				particles.add(change);
			}
		
			return particles;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	public Future<AtomCertificate> submit(Atom atom) throws InterruptedException
	{
		AtomFuture atomFuture;
		this.lock.lock();
		try
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

			atom.getParticles(TokenParticle.class).forEach(tp -> 
			{
				if (tp.getOwner().equals(SimpleWallet.this.key.getPublicKey()) == false)
					return;
				
				if (tp.getSpin().equals(Spin.DOWN) == true)
					SimpleWallet.this.unconsumed.remove(tp.get(Spin.UP));
			});
			
			return atomFuture;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	// ACTION LISTENER //
	private EventListener atomListener = new EventListener() 
	{
		@Subscribe
		public void on(final AtomAcceptedEvent event) 
		{
			accept(event.getAtom(), event.getPendingAtom().getCertificate());
		}
		
		@Subscribe
		public void on(final AtomRejectedEvent event) 
		{
			reject(event.getAtom(), event.getPendingAtom().getCertificate());
		}
		
		@Subscribe
		public void on(final AtomAcceptedTimeoutEvent event) 
		{
			timeout(event.getAtom());
		}

		@Subscribe
		public void on(final AtomCommitTimeoutEvent event) 
		{
			timeout(event.getAtom());
		}

		@Subscribe
		public void on(final AtomExceptionEvent event) 
		{
			exception(event.getAtom(), event.getException());
		}
	};
	
	private void accept(final Atom atom, final AtomCertificate certificate)
	{
		this.lock.lock();
		try
		{
			AtomFuture future = this.futures.remove(atom.getHash());
			if (future != null)
				future.complete(certificate);
			
			atom.getParticles().forEach(p -> 
			{
				if (p instanceof MessageParticle)
				{
					if (((MessageParticle)p).getSender().equals(this.key.getPublicKey()) == false && ((MessageParticle)p).getRecipient().equals(this.key.getPublicKey()) == false)
						return;
				}
				else if (p instanceof SignedParticle)
				{
					if (((SignedParticle)p).getOwner().equals(this.key.getPublicKey()) == false)
						return;
				}

				this.particles.put(p.getHash(), p);
				
				if (p instanceof TokenParticle)
				{
					if (p.getSpin().equals(Spin.UP) == true && ((TokenParticle)p).getAction().equals(Action.TRANSFER) == true)
						this.unconsumed.add((TokenParticle)p);
		
					if (p.getSpin().equals(Spin.DOWN) == true && ((TokenParticle)p).getAction().equals(Action.TRANSFER) == true)
						this.unconsumed.remove(p.get(Spin.UP));
				}	
			});
		}
		finally
		{
			this.lock.unlock();
		}
	}

	private void reject(final Atom atom, final AtomCertificate certificate)
	{
		this.lock.lock();
		try
		{
			AtomFuture future = this.futures.remove(atom.getHash());
			if (future != null)
				future.complete(certificate);
	
			atom.getParticles().forEach(p -> 
			{
				if (p instanceof MessageParticle)
				{
					if (((MessageParticle)p).getSender().equals(this.key.getPublicKey()) == false && ((MessageParticle)p).getRecipient().equals(this.key.getPublicKey()) == false)
						return;
				}
				else if (p instanceof SignedParticle)
				{
					if (((SignedParticle)p).getOwner().equals(this.key.getPublicKey()) == false)
						return;
				}
				
				this.particles.put(p.getHash(), p);
				
				if (p instanceof TokenParticle)
				{
					if (p.getSpin().equals(Spin.UP) == true && ((TokenParticle)p).getAction().equals(Action.TRANSFER) == true)
						this.unconsumed.remove((TokenParticle)p);
					
					if (p.getSpin().equals(Spin.DOWN) == true && ((TokenParticle)p).getAction().equals(Action.TRANSFER) == true)
						this.unconsumed.add(p.get(Spin.UP));
				}
			});
		}
		finally
		{
			this.lock.unlock();
		}
	}

	private void timeout(final Atom atom)
	{
		this.lock.lock();
		try
		{
			AtomFuture future = this.futures.remove(atom.getHash());
			if (future != null)
				future.complete(null);
	
			atom.getParticles(TokenParticle.class).forEach(tp -> 
			{
				if (tp.getOwner().equals(this.key.getPublicKey()) == false)
					return;
				
				if (tp.getSpin().equals(Spin.UP) == true && tp.getAction().equals(Action.TRANSFER) == true)
					this.unconsumed.remove(tp);
				
				if (tp.getSpin().equals(Spin.DOWN) == true && tp.getAction().equals(Action.TRANSFER) == true)
					this.unconsumed.add(tp.get(Spin.UP));
			});
		}
		finally
		{
			this.lock.unlock();
		}
	}

	private void exception(final Atom atom, final Exception exception)
	{
		this.lock.lock();
		try
		{
			AtomFuture future = this.futures.remove(atom.getHash());
			if (future != null)
				future.completeExceptionally(exception);
	
			atom.getParticles(TokenParticle.class).forEach(tp -> 
			{
				if (tp.getOwner().equals(this.key.getPublicKey()) == false)
					return;
				
				if (tp.getSpin().equals(Spin.UP) == true && tp.getAction().equals(Action.TRANSFER) == true)
					this.unconsumed.remove(tp);
				
				if (tp.getSpin().equals(Spin.DOWN) == true && tp.getAction().equals(Action.TRANSFER) == true)
					this.unconsumed.add(tp.get(Spin.UP));
			});
		}
		finally
		{
			this.lock.unlock();
		}
	}
}
