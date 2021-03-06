package org.fuserleer.console;

import java.io.PrintStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.fuserleer.Context;
import org.fuserleer.ledger.ShardMapper;
import org.fuserleer.network.discovery.OutboundShardDiscoveryFilter;
import org.fuserleer.network.discovery.OutboundSyncDiscoveryFilter;
import org.fuserleer.network.discovery.RemoteLedgerDiscovery;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.network.peers.Peer;
import org.fuserleer.network.peers.PeerState;
import org.fuserleer.network.peers.filters.NotLocalPeersFilter;
import org.fuserleer.network.peers.filters.StandardPeerFilter;

public class Network extends Function
{
	private final static Options options = new Options().addOption(Option.builder("disconnect").desc("Disconnect a peer").optionalArg(true).build())
														.addOption("stats", false, "Outputs network statistics")
														.addOption("known", false, "Lists all known peers (can be very large)") // TODO pagination for this?
														.addOption("best", false, "Lists all known peers by XOR ranking (can be very large)"); // TODO pagination for this?


	public Network()
	{
		super("network", options);
	}

	@Override
	public void execute(Context context, String[] arguments, PrintStream printStream) throws Exception
	{
		CommandLine commandLine = Function.parser.parse(options, arguments);

		if (commandLine.hasOption("disconnect") == true)
		{
			if (commandLine.getOptionValue("disconnect") == null)
			{
				for (ConnectedPeer connectedPeer : context.getNetwork().get(StandardPeerFilter.build(context).setStates(PeerState.CONNECTED, PeerState.CONNECTING)))
				{
					connectedPeer.disconnect("Forced disconnect");
					printStream.println("Disconnecting "+connectedPeer+" @ "+connectedPeer.getNode().getHead());
				}
			}
			else
			{
				printStream.println("Disconnecting individual peers not yet supported");
			}
		}
		else if (commandLine.hasOption("best") == true)
		{
			Collection<Peer> bestPeers = context.getNetwork().getPeerStore().get(new NotLocalPeersFilter(context.getNode()));
			for (Peer bestPeer : bestPeers)
				printStream.println((bestPeer.getNode().getIdentity().asHash().asLong() ^ context.getNode().getIdentity().asHash().asLong())+" "+bestPeer.toString());

			long syncShardGroup = ShardMapper.toShardGroup(context.getNode().getIdentity(), context.getLedger().numShardGroups());
			printStream.println("-- Filtered Sync "+syncShardGroup+" ---");
			OutboundSyncDiscoveryFilter outboundSyncDiscoveryFilter = new OutboundSyncDiscoveryFilter(context, Collections.singleton(syncShardGroup));
			bestPeers = new RemoteLedgerDiscovery(context).discover(outboundSyncDiscoveryFilter, Integer.MAX_VALUE);
			for (Peer bestPeer : bestPeers)
				printStream.println((bestPeer.getNode().getIdentity().asHash().asLong() ^ context.getNode().getIdentity().asHash().asLong())+" "+bestPeer.toString());

			for (long sg = 0 ; sg < context.getLedger().numShardGroups(context.getLedger().getHead().getHeight()) ; sg++)
			{
				long shardGroup = sg;
				if (shardGroup == syncShardGroup)
					continue;

				printStream.println("-- Filtered Shard "+sg+" ---");
				OutboundShardDiscoveryFilter outboundShardDiscoveryFilter = new OutboundShardDiscoveryFilter(context, Collections.singleton(shardGroup));
				bestPeers = new RemoteLedgerDiscovery(context).discover(outboundShardDiscoveryFilter, Integer.MAX_VALUE);
				for (Peer bestPeer : bestPeers)
					printStream.println((bestPeer.getNode().getIdentity().asHash().asLong() ^ context.getNode().getIdentity().asHash().asLong())+" "+bestPeer.toString());
			}
		}
		else if (commandLine.hasOption("known") == true)
		{
			// TODO paginate this
			Collection<Peer> knownPeers = context.getNetwork().getPeerStore().get(0, Short.MAX_VALUE, new NotLocalPeersFilter(context.getNode()));
			long numShardGroups = context.getLedger().numShardGroups();
			for (Peer knownPeer : knownPeers)
				printStream.println("S-"+(ShardMapper.toShardGroup(knownPeer.getNode().getIdentity(), numShardGroups))+" <- "+knownPeer.toString());
		}
		else if (commandLine.hasOption("stats") == true)
		{
			printStream.println("Bandwidth:");
			printStream.println("In bytes: "+context.getNetwork().getMessaging().getBytesIn()+" "+(context.getNetwork().getMessaging().getBytesIn() / TimeUnit.MILLISECONDS.toSeconds(context.uptime()))+" b/s");
			printStream.println("Out bytes: "+context.getNetwork().getMessaging().getBytesOut()+" "+(context.getNetwork().getMessaging().getBytesOut() / TimeUnit.MILLISECONDS.toSeconds(context.uptime()))+" b/s");

			printStream.println("Sent messages: "+context.getNetwork().getMessaging().getTotalSent());
			for (Entry<Class<?>, Long> sent : context.getNetwork().getMessaging().getSentByType())
				printStream.println(sent.getKey()+": "+sent.getValue());

			printStream.println("Received messages: "+context.getNetwork().getMessaging().getTotalReceived());
			for (Entry<Class<?>, Long> received : context.getNetwork().getMessaging().getReceivedByType())
				printStream.println(received.getKey()+": "+received.getValue());
		}
		else
		{
			long numShardGroups = context.getLedger().numShardGroups();
			long shardGroup = ShardMapper.toShardGroup(context.getNode().getIdentity(), context.getLedger().numShardGroups());
			printStream.println("Sync:");
			for (ConnectedPeer peer : context.getNetwork().get(StandardPeerFilter.build(context).setStates(PeerState.CONNECTED)))
			{
				if (ShardMapper.toShardGroup(peer.getNode().getIdentity(), context.getLedger().numShardGroups()) == shardGroup)
					printStream.println(peer.toString()+": "+peer.getNode().getHead().toString());
			}
	
			printStream.println("Shard:");
			for (ConnectedPeer peer : context.getNetwork().get(StandardPeerFilter.build(context).setStates(PeerState.CONNECTED)))
			{
				if (ShardMapper.toShardGroup(peer.getNode().getIdentity(), context.getLedger().numShardGroups()) != shardGroup)
					printStream.println(ShardMapper.toShardGroup(peer.getNode().getIdentity(), context.getLedger().numShardGroups())+" "+peer.toString()+": "+peer.getNode().getHead().toString());
			}
		}
	}
}