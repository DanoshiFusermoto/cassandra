package org.fuserleer.console;

import java.io.PrintStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.fuserleer.Context;
import org.fuserleer.network.discovery.RemoteLedgerDiscovery;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.network.peers.Peer;
import org.fuserleer.network.peers.PeerState;
import org.fuserleer.network.peers.filters.AllPeersFilter;
import org.fuserleer.network.peers.filters.OutboundTCPPeerFilter;

public class Network extends Function
{
	private final static Options options = new Options().addOption(Option.builder("disconnect").desc("Disconnect a peer").optionalArg(true).build())
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
				List<ConnectedPeer> connectedPeers = context.getNetwork().get(PeerState.CONNECTED, PeerState.CONNECTING);
				for (ConnectedPeer connectedPeer : connectedPeers)
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
			Collection<Peer> bestPeers = new RemoteLedgerDiscovery(context).discover(new AllPeersFilter());
			for (Peer bestPeer : bestPeers)
				printStream.println((bestPeer.getNode().getIdentity().asHash().asLong() ^ context.getNode().getIdentity().asHash().asLong())+" "+bestPeer.toString());

			printStream.println("-- Filtered ---");
			OutboundTCPPeerFilter outboundTCPPeerFilter = new OutboundTCPPeerFilter(context, Collections.singleton(context.getLedger().getShardGroup(context.getNode().getIdentity())));
			bestPeers = new RemoteLedgerDiscovery(context).discover(outboundTCPPeerFilter);
			for (Peer bestPeer : bestPeers)
				printStream.println((bestPeer.getNode().getIdentity().asHash().asLong() ^ context.getNode().getIdentity().asHash().asLong())+" "+bestPeer.toString());
		}
		else if (commandLine.hasOption("known") == true)
		{
			// TODO paginate this
			Collection<Peer> knownPeers = context.getNetwork().getPeerStore().get(0, Short.MAX_VALUE, new AllPeersFilter());
			for (Peer knownPeer : knownPeers)
				printStream.println(knownPeer.toString());
		}
		else
		{
			printStream.println("Connected:");
			for (ConnectedPeer peer : context.getNetwork().get(PeerState.CONNECTED))
				printStream.println(peer.toString()+": "+peer.getNode().getHead().toString());
	
			printStream.println("Bandwidth:");
			printStream.println("In bytes: "+context.getNetwork().getMessaging().getBytesIn());
			printStream.println("Out bytes: "+context.getNetwork().getMessaging().getBytesOut());
		}
	}
}