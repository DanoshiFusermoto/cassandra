package org.fuserleer.network.discovery;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.Context;
import org.fuserleer.crypto.Hash;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.Network;
import org.fuserleer.network.peers.Peer;
import org.fuserleer.network.peers.filters.PeerFilter;
import org.fuserleer.utils.MathUtils;

public class RemoteLedgerDiscovery implements Discovery
{
	private static final Logger networkLog = Logging.getLogger("network");

	private Context context;
	
	public RemoteLedgerDiscovery(Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
	}
	
	@Override
	// TODO optimization of peer selection.  currently gets all peers from peerstore and sorts for each locator attempt
	public Collection<Peer> discover(PeerFilter filter) throws IOException
	{
		final Hash locator = this.context.getNode().getIdentity().asHash();
		final List<Peer> known = this.context.getNetwork().getPeerStore().get(filter);
		if (known.isEmpty() == true)
			return Collections.emptyList();
		
		Collections.sort(known, new Comparator<Peer>() 
		{
			@Override
			public int compare(Peer arg0, Peer arg1)
			{
				long distance0 = MathUtils.ringDistance64(locator.asLong(), arg0.getNode().getIdentity().asHash().asLong());
				long distance1 = MathUtils.ringDistance64(locator.asLong(), arg1.getNode().getIdentity().asHash().asLong());
				
				if (distance0 < distance1)
					return -1;
				
				if (distance0 > distance1)
					return 1;

				return arg0.getNode().getIdentity().asHash().compareTo(arg1.getNode().getIdentity().asHash());
			}
		});
		
 		Set<Peer> discovered = new LinkedHashSet<Peer>(known.subList(0, Math.min(known.size(), this.context.getConfiguration().get("network.connections.out", Network.DEFAULT_TCP_CONNECTIONS_OUT))));
		return discovered;
	}
}
