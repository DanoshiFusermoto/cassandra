package org.fuserleer.ledger;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.Universe;
import org.fuserleer.collections.LRUCacheMap;
import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;

final class BlockRegulator implements Service
{
	private static final Logger blocksLog = Logging.getLogger("blocks");

	// TODO put these in universe config
	public final static long 	BASELINE_DISTANCE_FACTOR = 4;
	public final static long 	BASELINE_DISTANCE_TARGET = BigInteger.valueOf(Long.MIN_VALUE).abs().subtract(BigInteger.valueOf((long) (Long.MIN_VALUE / (BASELINE_DISTANCE_FACTOR * Math.log(BASELINE_DISTANCE_FACTOR)))).abs()).longValue(); // TODO rubbish, but produces close enough needed output
	public final static long 	BLOCK_DISTANCE_FACTOR = 64;
	public final static long 	BLOCK_DISTANCE_TARGET = BigInteger.valueOf(Long.MIN_VALUE).abs().subtract(BigInteger.valueOf((long) (Long.MIN_VALUE / (BLOCK_DISTANCE_FACTOR * Math.log(BLOCK_DISTANCE_FACTOR)))).abs()).longValue(); // TODO rubbish, but produces close enough needed output
	public final static int		BLOCKS_PER_PERIOD = 60;
	public final static int 	BLOCK_PERIOD_DURATION = 600;

	private final Context context;
	private final Map<Long, Long> computedCache;
	
	BlockRegulator(final Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.computedCache = Collections.synchronizedMap(new LRUCacheMap<Long, Long>(BLOCKS_PER_PERIOD*2, false));
	}

	@Override
	public void start() throws StartupException
	{
		try
		{
			this.computedCache.clear();
	
			BlockHeader head = this.context.getLedger().getHead();
			BlockHeader current = head;
			do
			{
				this.computedCache.put(current.getHeight(), current.getTarget());
	
				long prevHeight = current.getHeight()-1;
				current = null;
				if (prevHeight >= 0)
				{
					Hash prev = this.context.getLedger().getLedgerStore().get(prevHeight);
					current = this.context.getLedger().getLedgerStore().get(prev, BlockHeader.class);
					if (current == null)
						throw new IllegalStateException("Previous committed block header at height "+prevHeight+" not found");
				}
			}
			while(current != null && current.getHeight() > head.getHeight() - BlockRegulator.BLOCKS_PER_PERIOD*2);
		}
		catch (Throwable t)
		{
			throw new StartupException(t);
		}
	}

	@Override
	public void stop() throws TerminationException
	{
		// TODO Auto-generated method stub
		
	}
	
	public long computeTarget(final BlockHeader header, final PendingBranch branch) throws IOException
	{
		Objects.requireNonNull(header, "Header is null");
		
		if (header.getHeight() < BlockRegulator.BLOCKS_PER_PERIOD)
			return Universe.getDefault().getGenesis().getHeader().getTarget();
		
		synchronized(this.computedCache)
		{
			if (this.computedCache.containsKey(header.getHeight()) == true)
				return this.computedCache.get(header.getHeight());
				
			long endTimestamp = 0;
			long startTimestamp = 0;
			
			BlockHeader current = header;
			// Align
			while(current.getHeight() % BlockRegulator.BLOCKS_PER_PERIOD != (BlockRegulator.BLOCKS_PER_PERIOD-1))
			{
				BlockHeader previous;
				if (branch != null && current.getHeight() > branch.getFirst().getHeight())
					previous = branch.get(current.getPrevious()).getHeader();
				else
					previous = this.context.getLedger().getLedgerStore().get(current.getPrevious(), BlockHeader.class); 
				
				if (previous == null)
					throw new IllegalStateException("Previous header not found for "+current.toString());
				
				current = previous;
			}

			BlockHeader endHeader = current;
			endTimestamp = current.getTimestamp();
			
			// Period (special case for genesis)
			while(current.getHeight() > 1 && current.getHeight() % BlockRegulator.BLOCKS_PER_PERIOD != 0)
			{
				BlockHeader previous;
				if (branch != null && current.getHeight() > branch.getFirst().getHeight())
					previous = branch.get(current.getPrevious()).getHeader();
				else
					previous = this.context.getLedger().getLedgerStore().get(current.getPrevious(), BlockHeader.class); 
				
				if (previous == null)
					throw new IllegalStateException("Previous header not found for "+current.toString());
				
				current = previous;
			}
			
			BlockHeader startHeader = current;
			startTimestamp = current.getTimestamp();
			
			long periodDelta = endTimestamp - startTimestamp;
			long adjustedTarget = startHeader.getTarget();
			if (periodDelta < TimeUnit.SECONDS.toMillis(BlockRegulator.BLOCK_PERIOD_DURATION))
			{
				long adjustmentDelta = (Long.MAX_VALUE - startHeader.getTarget()) / 2;
				adjustedTarget = startHeader.getTarget() + adjustmentDelta;
			}
			else if (periodDelta > TimeUnit.SECONDS.toMillis(BlockRegulator.BLOCK_PERIOD_DURATION))
			{
				long adjustmentDelta = (Long.MAX_VALUE - startHeader.getTarget()) / 2;
				adjustedTarget = startHeader.getTarget() - adjustmentDelta;
			}
			
			blocksLog.info(this.context.getName()+": Computed block regulator target @ "+header.getHeight()+" from "+startHeader.getTarget()+" to "+adjustedTarget+" with period duration "+startHeader.getHeight()+" -> "+endHeader.getHeight()+" is "+periodDelta);
			this.computedCache.put(header.getHeight(), adjustedTarget);
			return adjustedTarget;
		}
	}
}
