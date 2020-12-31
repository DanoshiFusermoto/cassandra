package org.fuserleer.ledger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.Universe;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.events.SynchronousEventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.ledger.atoms.ParticleCertificate;
import org.fuserleer.ledger.events.BlockCommittedEvent;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.utils.UInt128;

import com.google.common.eventbus.Subscribe;

public final class VoteRegulator implements Service
{
	private static final Logger powerLog = Logging.getLogger("power");

	private final Context context;
	private final Map<ECPublicKey, Long> votePower;
	
	VoteRegulator(final Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.votePower = Collections.synchronizedMap(new HashMap<ECPublicKey, Long>());
	}
	
	@Override
	public void start() throws StartupException
	{
		this.votePower.clear();
		for (ECPublicKey genode : Universe.getDefault().getGenodes())
		{
			powerLog.info(this.context.getName()+": Setting vote power for genesis node "+genode+":"+this.context.getLedger().getShardGroup(genode)+" to "+1);
			this.votePower.put(genode, 1l);
		}
		
		this.context.getEvents().register(this.syncBlockListener);
	}

	@Override
	public void stop() throws TerminationException
	{
		this.context.getEvents().unregister(this.syncBlockListener);
	}
	
	public long getVotePower(ECPublicKey identity)
	{
		return this.votePower.getOrDefault(identity, 0l);
	}
	
	public VotePowerBloom getVotePowerBloom(UInt128 shardGroup)
	{
		VotePowerBloom votePowerBloom = new VotePowerBloom();
		synchronized(this.votePower)
		{
			for (Entry<ECPublicKey, Long> votePower : this.votePower.entrySet())
			{
				if (this.context.getLedger().getShardGroup(votePower.getKey()).compareTo(shardGroup) != 0)
					continue;
				
				votePowerBloom.add(votePower.getKey(), votePower.getValue());
			}
		}

		return votePowerBloom;
	}

	public long getTotalVotePower(Set<UInt128> shardGroups)
	{
		synchronized(this.votePower)
		{
			long totalVotePower = 0;
			for (Entry<ECPublicKey, Long> votePower : this.votePower.entrySet())
			{
				if (shardGroups.contains(this.context.getLedger().getShardGroup(votePower.getKey())) == false)
					continue;

				totalVotePower += votePower.getValue(); 
			}
			return totalVotePower;
		}
	}
	
	public long getVotePower(Set<ECPublicKey> identities)
	{
		long votePower = 0l;
		for (ECPublicKey identity : identities)
			votePower += this.votePower.getOrDefault(identity, 0l);
		
		return votePower;
	}

	public long getVotePowerThreshold(Set<UInt128> shardGroups)
	{
		return twoFPlusOne(getTotalVotePower(shardGroups));
	}
	
	long addVotePower(ECPublicKey identity)
	{
		this.votePower.put(identity, this.votePower.getOrDefault(identity, 0l) + 1l);
		powerLog.info(this.context.getName()+": Incrementing vote power for "+identity+":"+this.context.getLedger().getShardGroup(identity)+" to "+this.votePower.get(identity));
		
		return this.votePower.getOrDefault(identity, 0l);
	}

	private long twoFPlusOne(long power)
	{
		long F = power / 3;
		long T = F * 2;
		return T + 1;
	}
	
	private void processVoteBloom(Set<ECPublicKey> identities, VotePowerBloom votePowerBloom)
	{
		synchronized(this.votePower)
		{
			for (ECPublicKey identity : identities)
			{
				long power = votePowerBloom.power(identity);
				if (power > this.votePower.getOrDefault(identity, 0l))
				{
					this.votePower.put(identity, power);
					powerLog.info(this.context.getName()+": Setting vote power for "+identity+":"+this.context.getLedger().getShardGroup(identity)+" to "+power);
				}
			}
		}
	}
	
	// BLOCK LISTENER //
	private SynchronousEventListener syncBlockListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(BlockCommittedEvent blockCommittedEvent) 
		{
			for (AtomCertificate atomCertificate : blockCommittedEvent.getBlock().getCertificates())
			{
				for (ParticleCertificate particleCertificate : atomCertificate.getAll())
					processVoteBloom(particleCertificate.getSignatures().getSigners(), particleCertificate.getVotePowers());
			}
		}
	};
}
