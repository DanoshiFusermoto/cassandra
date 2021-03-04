package org.fuserleer.network.discovery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.fuserleer.Context;
import org.fuserleer.crypto.Hash;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.peers.Peer;

public class RemoteLedgerDiscovery implements Discovery
{
	private static final Logger networkLog = Logging.getLogger("network");

	private Context context;
	
	public RemoteLedgerDiscovery(final Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
	}
	
	@Override
	// TODO optimisation of peer selection.  currently gets all peers from peerstore and sorts for each locator attempt
	public Collection<Peer> discover(final DiscoveryFilter filter, int limit) throws IOException
	{
		Objects.requireNonNull(filter, "Discovery filter is null");
		if (limit < 0)
			throw new IllegalArgumentException("Discovery limit is negative");
		
		final Hash locator = this.context.getNode().getIdentity().asHash();
		final List<Peer> known = this.context.getNetwork().getPeerStore().get(filter);
		if (known.isEmpty() == true)
			return Collections.emptyList();
		
		if (networkLog.hasLevel(Logging.DEBUG) == true)
			networkLog.debug(this.context.getName()+": Discovering from "+known.size()+" known peers");
		
		Collections.sort(known, new Comparator<Peer>() 
		{
			@Override
			public int compare(Peer arg0, Peer arg1)
			{
				long xor0 = arg0.getNode().getIdentity().asHash().asLong() ^ locator.asLong(); // * locator.asLong());
				long xor1 = arg1.getNode().getIdentity().asHash().asLong() ^ locator.asLong(); // * locator.asLong());
				
				if (Math.abs(xor0) < Math.abs(xor1))
					return -1;
				
				if (Math.abs(xor0) > Math.abs(xor1))
					return 1;

				return arg0.getNode().getIdentity().asHash().compareTo(arg1.getNode().getIdentity().asHash());
			}
		});
		
		List<Peer> discovered = new ArrayList<Peer>(known.subList(0, Math.min(known.size(), limit)));
		return discovered;
	}
}
