package org.fuserleer.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.fuserleer.Context;
import org.fuserleer.collections.Bloom;
import org.fuserleer.crypto.BLSPublicKey;
import org.fuserleer.database.DatabaseException;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.utils.Numbers;

final class SyncBranch
{
	private static final Logger syncLog = Logging.getLogger("sync");

	private final Context context;
	private final LinkedList<BlockHeader> headers;
	
	private BlockHeader root;

	private final ReentrantLock lock = new ReentrantLock();
	
	SyncBranch(final Context context, final BlockHeader root) throws IOException
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.headers = new LinkedList<BlockHeader>();
		this.root = Objects.requireNonNull(root, "Root is null");
	}
	
	boolean push(final BlockHeader header) throws StateLockedException, IOException
	{
		Objects.requireNonNull(header, "Block header is null");
	
		this.lock.lock();
		try
		{
			if (header.getHeight() <= this.root.getHeight())
				syncLog.warn(this.context.getName()+": Block header "+header.getHash()+" is before branch root "+this.root.getHash());
			
			this.headers.add(header);
			if (syncLog.hasLevel(Logging.DEBUG) == true)
				syncLog.info(context.getName()+": Pushed block header "+header+" to branch on "+this.root);
			
			Collections.sort(this.headers, new Comparator<BlockHeader>()
			{
				@Override
				public int compare(BlockHeader arg0, BlockHeader arg1)
				{
					return (int) (arg0.getHeight() - arg1.getHeight());
				}
			});
			
			return true;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	/**
	 * Trims the branch to the block header (inclusive)
	 * 
	 * @param header
	 */
	void trimTo(final BlockHeader header)
	{
		Objects.requireNonNull(header, "Block is null");

		this.lock.lock();
		try
		{
			Iterator<BlockHeader> vertexIterator = this.headers.iterator();
			while(vertexIterator.hasNext() == true)
			{
				BlockHeader vertex = vertexIterator.next();
				if (vertex.getHeight() <= header.getHeight())
					vertexIterator.remove();
			}

			this.root = header;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	BlockHeader commitable() throws IOException, StateLockedException
	{
		this.lock.lock();
		try
		{
			if (isCanonical() == false)
				return null;
			
			// See if there is a section of the best branch that can be committed (any block that has 2f+1 agreement)
			// Blocks to be committed require at least one "confirming" super block higher than it, thus there will always 
			// be at least one super block in a pending branch
			// TODO using pendingBlock.getHeader().getHeight() as the vote power timestamp possibly makes this weakly subjective and may cause issue in long branches
			LinkedList<BlockHeader> supers = new LinkedList<BlockHeader>();
			Iterator<BlockHeader> vertexIterator = this.headers.descendingIterator();
			while(vertexIterator.hasNext())
			{
				BlockHeader vertex = vertexIterator.next();
				if (vertex.getCertificate() == null)
					continue;
				
				long weight = getVotePower(vertex.getHeight(), vertex.getCertificate().getSigners());
				long total = getTotalVotePower(vertex.getHeight());
				long threshold = getVotePowerThreshold(vertex.getHeight());
				if (weight >= threshold)
				{
					if (supers.isEmpty() || supers.size() < Math.ceil(Math.log(size())))
					{
						supers.add(vertex);
						syncLog.info(this.context.getName()+": Found possible commit super block with weight "+weight+"/"+total+" "+vertex);
					}
					else
					{
						syncLog.info(this.context.getName()+": Found commit at block with weight "+weight+"/"+total+" to commit list "+vertex);
						return vertex;
					}
				}
			}
			
			return null;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	List<BlockHeader> supers() throws IOException, StateLockedException
	{
		this.lock.lock();
		try
		{
			List<BlockHeader> supers = new ArrayList<BlockHeader>();
			if (isCanonical() == false)
				return null;
			
			Iterator<BlockHeader> vertexIterator = this.headers.iterator();
			while(vertexIterator.hasNext())
			{
				BlockHeader vertex = vertexIterator.next();
				if (vertex.getCertificate() == null)
					continue;
				
				long weight = getVotePower(vertex.getHeight(), vertex.getCertificate().getSigners());
				long threshold = getVotePowerThreshold(vertex.getHeight());
				if (weight >= threshold)
					supers.add(vertex);
			}
			
			return supers;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	BlockHeader getRoot()
	{
		this.lock.lock();
		try
		{
			return this.root;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	LinkedList<BlockHeader> getHeaders()
	{
		this.lock.lock();
		try
		{
			return new LinkedList<BlockHeader>(this.headers);
		}
		finally
		{
			this.lock.unlock();
		}
	}

	boolean isEmpty()
	{
		this.lock.lock();
		try
		{
			return this.headers.isEmpty();
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	boolean isCanonical()
	{
		this.lock.lock();
		try
		{
			BlockHeader current = null;
			Iterator<BlockHeader> headerIterator = this.headers.descendingIterator();
			while(headerIterator.hasNext() == true)
			{
				BlockHeader previous = headerIterator.next();
				if (current != null)
				{
					if (previous.getHash().equals(current.getPrevious()) == false)
						return false;
					
					current = previous;
				}
				
				current = previous;
				if (current.getPrevious().equals(this.root.getHash()) == true)
					return true;
			}
			
			return false;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	BlockHeader getLow()
	{
		this.lock.lock();
		try
		{
			return this.headers.getFirst();
		}
		finally
		{
			this.lock.unlock();
		}
	}

	BlockHeader getHigh()
	{
		this.lock.lock();
		try
		{
			return this.headers.getLast();
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	@Override
	public String toString()
	{
		return getHeaders().stream().map(pb -> pb.getHash().toString()).collect(Collectors.joining(" -> "));
	}

	public int size()
	{
		this.lock.lock();
		try
		{
			return this.headers.size();
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	// Vote power and weights //
	long getVotePower(final long height, final BLSPublicKey identity) throws IOException
	{
		Objects.requireNonNull(identity, "Identity is null");
		Numbers.isNegative(height, "Height is negative");
		
		long committedPower = this.context.getLedger().getValidatorHandler().getVotePower(Math.max(0, height - ValidatorHandler.VOTE_POWER_MATURITY), identity);
		long pendingPower = 0;
		this.lock.lock();
		try
		{
			for (BlockHeader vertex : this.headers)
			{
				if (vertex.getHeight() <= height - ValidatorHandler.VOTE_POWER_MATURITY && vertex.getOwner().equals(identity) == true)
					pendingPower++;
			}
		}
		finally
		{
			this.lock.unlock();
		}
		
		return committedPower+pendingPower;
	}

	public long getVotePower(final long height, final Bloom owners) throws DatabaseException
	{
		Objects.requireNonNull(owners, "Identities is null");

		long committedPower = this.context.getLedger().getValidatorHandler().getVotePower(Math.max(0, height - ValidatorHandler.VOTE_POWER_MATURITY), owners);
		long pendingPower = 0;
		this.lock.lock();
		try
		{
			for (BlockHeader vertex : this.headers)
			{
				if (vertex.getHeight() <= height - ValidatorHandler.VOTE_POWER_MATURITY)
				{
					if (owners.contains(vertex.getOwner().toByteArray()) == true)
						pendingPower++;
				}
			}
		}
		finally
		{
			this.lock.unlock();
		}
		
		return committedPower+pendingPower;
	}

	long getVotePowerThreshold(final long height) throws IOException
	{
		Numbers.isNegative(height, "Height is negative");
		return twoFPlusOne(getTotalVotePower(height));
	}

	long getTotalVotePower(final long height) throws IOException
	{
		Numbers.isNegative(height, "Height is negative");
		
		long localShardGroup = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), this.context.getLedger().numShardGroups()); 
		long committedPower = this.context.getLedger().getValidatorHandler().getTotalVotePower(Math.max(0, height - ValidatorHandler.VOTE_POWER_MATURITY), localShardGroup);
		long pendingPower = 0;
		this.lock.lock();
		try
		{
			for (BlockHeader vertex : this.headers)
			{
				if (vertex.getHeight() <= height - ValidatorHandler.VOTE_POWER_MATURITY)
					pendingPower++;
			}
		}
		finally
		{
			this.lock.unlock();
		}
		
		return committedPower+pendingPower;
	}
	
	private long twoFPlusOne(final long power)
	{
		Numbers.isNegative(power, "Power is negative");

		long F = Math.max(1, power / 3);
		long T = F * 2;
		return Math.min(power, T + 1);
	}
}
