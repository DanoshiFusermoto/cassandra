package org.fuserleer.network.peers.filters;

import java.net.URI;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.fuserleer.Context;
import org.fuserleer.common.Direction;
import org.fuserleer.crypto.PublicKey;
import org.fuserleer.ledger.ShardMapper;
import org.fuserleer.network.Protocol;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.network.peers.PeerState;
import org.fuserleer.utils.Numbers;

public class StandardPeerFilter implements PeerFilter<ConnectedPeer>
{
	private final Context 	context;
	private Direction		direction;
	private Long 			shardGroup;
	private Protocol 		protocol;
	private PublicKey		identity;
	private URI				URI;
	private Set<PeerState> 	states;
	private Boolean			synced;
	
	public static final StandardPeerFilter build(final Context context)
	{
		return new StandardPeerFilter(context);
	}

	private StandardPeerFilter(final Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
	}
	

	public StandardPeerFilter setSynced(final boolean synced)
	{
		this.synced = synced;
		return this;
	}

	public StandardPeerFilter setURI(final URI URI)
	{
		this.URI = Objects.requireNonNull(URI, "URI is null");
		return this;
	}

	public StandardPeerFilter setIdentity(final PublicKey identity)
	{
		this.identity = Objects.requireNonNull(identity, "Direction is null");
		return this;
	}

	public StandardPeerFilter setDirection(final Direction direction)
	{
		this.direction = Objects.requireNonNull(direction, "Direction is null");
		return this;
	}

	public StandardPeerFilter setShardGroup(final long shardGroup)
	{
		this.shardGroup =  Objects.requireNonNull(shardGroup, "Shard group is null");
		return this;
	}

	public StandardPeerFilter setProtocol(final Protocol protocol)
	{
		this.protocol =  Objects.requireNonNull(protocol, "Protocol is null");
		return this;
	}

	public StandardPeerFilter setStates(final PeerState ... states)
	{
		Objects.requireNonNull(states, "States is null");
		Numbers.isZero(states.length, "Peer states is empty");
		
		this.states = Arrays.stream(states).collect(Collectors.toSet());
		return this;
	}

	@Override
	public boolean filter(final ConnectedPeer peer)
	{
		Objects.requireNonNull(peer, "Connected peer is null");
		
		if (this.synced != null && this.synced != peer.getNode().isSynced())
			return false;
		
		if (this.shardGroup != null && ShardMapper.toShardGroup(peer.getNode().getIdentity(), this.context.getLedger().numShardGroups()) != this.shardGroup)
			return false;

		if (this.protocol != null && peer.hasProtocol(this.protocol) == false)
			return false;
		
		if (this.identity != null && peer.getNode().getIdentity().equals(this.identity) == false)
			return false;

		if (this.URI != null && peer.getURI().equals(this.URI) == false)
			return false;

		if (this.direction != null && peer.getDirection().equals(this.direction) == false)
			return false;

		if (this.states != null && this.states.contains(peer.getState()) == false)
			return false;

		return true;
	}
}
