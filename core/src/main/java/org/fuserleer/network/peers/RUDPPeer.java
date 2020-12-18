package org.fuserleer.network.peers;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.fuserleer.Context;
import org.fuserleer.common.Direction;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.Protocol;
import org.fuserleer.network.messages.FlowControlMessage;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.Polymorphic;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("network.peer")
/**
 *  Simple implementation of a more reliable UDP peer that implements basic congestion handling.
 *  
 *  TODO needs to be much improved with adjustments to the throughput if the connection is congested
 *  ordering of packets, and configurable parameters for timeouts etc
 *
 */
public final class RUDPPeer extends ConnectedPeer implements Polymorphic
{
	private static final Logger networkLog = Logging.getLogger("network");
	private static final Logger messagingLog = Logging.getLogger("messaging");

	private final DatagramChannel	socket;
	
	private final class FlowControlRecord
	{
		private final long sequence;
		private final byte[] data;
		private final long timestamp;
		
		FlowControlRecord(long sequence, byte[] data, long timestamp)
		{
			this.sequence = sequence;
			this.data = Objects.requireNonNull(data);
			this.timestamp = timestamp;
		}

		public long getSequence()
		{
			return this.sequence;
		}

		public byte[] getData()
		{
			return this.data;
		}

		public long getTimestamp()
		{
			return this.timestamp;
		}

		@Override
		public int hashCode()
		{
			return (int) this.sequence;
		}

		@Override
		public boolean equals(Object object)
		{
			if (object == null || (object instanceof FlowControlRecord) == false)
				return false;
			
			if (object == this)
				return true;
			
			if (((FlowControlRecord)object).sequence == this.sequence &&
				((FlowControlRecord)object).timestamp == this.timestamp &&	
				Arrays.equals(((FlowControlRecord)object).data, this.data) == true)
				return true;
			
			return false;
		}
	}
	
	private final Semaphore bufferRemaining = new Semaphore(1<<17);
	private final Map<Long, FlowControlRecord>	buffer = Collections.synchronizedMap(new HashMap<Long, FlowControlRecord>());
	private final Set<Long>	sequencesToAck = Collections.synchronizedSet(new LinkedHashSet<Long>());
	private final AtomicLong lastSequencesAck = new AtomicLong(0);
	private final AtomicLong lastBufferMaintenance = new AtomicLong(0);

	public RUDPPeer(Context context, DatagramChannel socket, URI host, Direction direction, Peer peer) throws IOException
	{
		super(context, host, direction, peer);

		this.socket = socket;
		addProtocol(Protocol.UDP);
	}

	@Override
	public Protocol getProtocol()
	{
		return Protocol.UDP;
	}
	
	@Override
	public void receive(Message message) throws IOException
	{
		if (message instanceof FlowControlMessage)
		{
			if (networkLog.hasLevel(Logging.DEBUG) == true)
				networkLog.debug(getContext().getName()+": "+this+" received RUDP FCM "+message.getHash()+" with "+((FlowControlMessage)message).getSequences().size()+" sequences");

			long seqHigh = 0;
			for (Long seq : ((FlowControlMessage)message).getSequences())
			{
				FlowControlRecord record = this.buffer.remove(seq);
				if (record != null)
					this.bufferRemaining.release(record.getData().length);
				
				if (seqHigh < seq)
					seqHigh = seq;
			}
			
			if (networkLog.hasLevel(Logging.DEBUG) == true)
				networkLog.debug(getContext().getName()+": "+this+" "+message.getHash()+" high seq is "+seqHigh);

			synchronized(this.buffer)
			{
				Iterator<Entry<Long, FlowControlRecord>> bufferIterator = this.buffer.entrySet().iterator();
				while(bufferIterator.hasNext() == true)
				{
					Entry<Long, FlowControlRecord> recordEntry = bufferIterator.next();
					if (recordEntry.getKey() < seqHigh)
					{
						bufferIterator.remove();
						this.bufferRemaining.release(recordEntry.getValue().getData().length);
						
						if (networkLog.hasLevel(Logging.DEBUG) == true)
							networkLog.debug(getContext().getName()+": "+this+" message with seq "+recordEntry.getValue().sequence+" sent at "+recordEntry.getValue().getTimestamp()+" is unacknowledged may have been lost en route to "+this);
					}
				}
			}
		}
		else
		{
			if (networkLog.hasLevel(Logging.DEBUG) == true)
				networkLog.debug(getContext().getName()+": "+this+" received RUDP message "+message.getHash()+" with seq "+message.getSeq());

			this.sequencesToAck.add(message.getSeq());
			maintain();
		}
	}

	@Override
	public void send(Message message) throws IOException
	{
		try
		{
			byte[] bytes = Message.prepare(message, getContext().getNode().getKey());
			if (bytes.length > 65535)
				throw new IOException("Datagram packet to "+this+" of size "+bytes.length+" is too large");
	
			ByteBuffer buf = ByteBuffer.wrap(bytes);
			InetSocketAddress addr = new InetSocketAddress(InetAddress.getByName(getURI().getHost()), getURI().getPort());
			
			if ((message instanceof FlowControlMessage) == false)
				maintain();
			
			int sent = 0;
			if (message instanceof FlowControlMessage || this.bufferRemaining.tryAcquire(bytes.length, 1, TimeUnit.SECONDS) == true)
			{
				try
				{
					sent = this.socket.send(buf, addr);
					if (sent != bytes.length)
						throw new IOException("Message to "+this+" was not completely sent "+sent+"/"+bytes.length+" bytes");
					
					if ((message instanceof FlowControlMessage) == false)
						this.buffer.put(message.getSeq(), new FlowControlRecord(message.getSeq(), bytes, System.currentTimeMillis()));
				}
				catch (Exception ex)
				{
					this.bufferRemaining.release(bytes.length);
					throw ex;
				}
			}
			else
				throw new TimeoutException();
		}
		catch (IOException ioex)
		{
			throw ioex;
		}
		catch (TimeoutException | InterruptedException iex)
		{
			throw new IOException("Message sending timed out or interrupted", iex);
		}
		catch (CryptoException cex)
		{
			throw new IOException("Message signing failed", cex);
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}
	
	private void maintain() throws IOException
	{
		if (this.sequencesToAck.isEmpty() == false && 
			(this.sequencesToAck.size() > 127 || System.currentTimeMillis() - this.lastSequencesAck.get() > 100))
		{
			synchronized (this.sequencesToAck)
			{
				List<Long> sequencesToAck = new ArrayList<Long>();
				Iterator<Long> sequencesToAckIterator = this.sequencesToAck.iterator();
				while (sequencesToAckIterator.hasNext() == true)
				{
					Long sequence = sequencesToAckIterator.next();
					sequencesToAck.add(sequence);
					sequencesToAckIterator.remove();
					
					if (sequencesToAck.size() == 128)
					{
						FlowControlMessage flowControlMessage = new FlowControlMessage(sequencesToAck);
						send(flowControlMessage);
						if (networkLog.hasLevel(Logging.DEBUG) == true)
							networkLog.debug(getContext().getName()+": "+this+" sent RUDP FCM "+flowControlMessage.getHash()+" with "+flowControlMessage.getSequences().size()+" sequences");
						sequencesToAck.clear();
					}
				}
				
				if (sequencesToAck.isEmpty() == false)
				{
					FlowControlMessage flowControlMessage = new FlowControlMessage(sequencesToAck);
					send(flowControlMessage);
					if (networkLog.hasLevel(Logging.DEBUG) == true)
						networkLog.debug(getContext().getName()+": "+this+" sent RUDP FCM "+flowControlMessage.getHash()+" with "+flowControlMessage.getSequences().size()+" sequences");
				}
			}
			
			this.lastSequencesAck.set(System.currentTimeMillis());
		}

		if (System.currentTimeMillis() - this.lastBufferMaintenance.get() > 100)
		{
			synchronized(this.buffer)
			{
				Iterator<Entry<Long, FlowControlRecord>> bufferIterator = this.buffer.entrySet().iterator();
				while(bufferIterator.hasNext() == true)
				{
					Entry<Long, FlowControlRecord> recordEntry = bufferIterator.next();
					if (System.currentTimeMillis() - recordEntry.getValue().timestamp > 1000)
					{
						bufferIterator.remove();
						this.bufferRemaining.release(recordEntry.getValue().getData().length);
						
						if (networkLog.hasLevel(Logging.DEBUG) == true)
							networkLog.debug(getContext().getName()+": "+this+" message with seq "+recordEntry.getValue().sequence+" sent at "+recordEntry.getValue().getTimestamp()+" is unacknowledged may have been lost en route to "+this);
					}
				}
			}
			
			this.lastBufferMaintenance.set(System.currentTimeMillis());
		}
	}

	@Override
	public void connect() throws IOException
	{
		onConnecting();
	}
}