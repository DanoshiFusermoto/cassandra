package org.fuserleer.network.messaging;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

import org.fuserleer.BasicObject;
import org.fuserleer.common.Direction;
import org.fuserleer.crypto.ECKeyPair;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.ECSignature;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.Universe;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.BanException;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.time.Time;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializerId2;
import org.xerial.snappy.Snappy;

import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class Message extends BasicObject
{
	private static final Logger messaginglog = Logging.getLogger("messaging");

	public static final int MAX_MESSAGE_SIZE = 1<<22;  // 4MB max message size

	private static final AtomicLong nextSeq = new AtomicLong(0);
	
	public static Message parse(InputStream inputStream) throws IOException, BanException, Exception
	{
		DataInputStream dataInputStream = new DataInputStream(inputStream);

		// Save the data items so that we can pre-calculate the hash //
		boolean compressed = dataInputStream.readBoolean();
		int length = dataInputStream.readInt();
		int read = 0;
		int total = 0;
		byte[] bytes = new byte[length];
		while(total != length && (read = dataInputStream.read(bytes, total, length-total)) != -1)
			total += read;	
		
		if (bytes.length > Message.MAX_MESSAGE_SIZE)
			throw new IOException("Message of size "+bytes.length+" is too large");
		
		if (compressed == true)
			bytes = Snappy.uncompress(bytes);
	
		Message message = Serialization.getInstance().fromDson(bytes, Message.class);
		if (message.getMagic() != Universe.getDefault().getMagic())
			throw new BanException("Wrong magic for this deployment");

		message.setDirection(Direction.INBOUND);
		message.size = length + 4 + 1;
		if (message.getSignature() == null)
			throw new BanException("Message "+message.getCommand()+" doesnt not include signature");

		return message;
	}
	
	public static byte[] prepare(Message message, ECKeyPair identity) throws CryptoException, IOException
	{
		if (message.getSignature() == null || message.getSender() == null)
			message.sign(identity);
		
		return message.toByteArray();
	}

	@JsonProperty("magic")
	@DsonOutput(value = Output.HASH, include = false)
	private long magic = Universe.getDefault().getMagic();

	@JsonProperty("seq")
	@DsonOutput(value = Output.HASH, include = false)
	private long seq = Message.nextSeq.incrementAndGet();

	@JsonProperty("sender")
	@DsonOutput(Output.ALL)
	// TODO can possibly remove this field as nodes should know who the message is coming from
	private ECPublicKey	sender;

	@JsonProperty("signature")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private ECSignature signature;

	@JsonProperty("timestamp")
	@DsonOutput(Output.ALL)
	private long timestamp = 0;

	// Transients //
	private transient 	int			size = 0;
	private transient 	Direction 	direction;
	private transient 	byte[]		bytes;
	
	protected Message()
	{
		super();
		
		this.timestamp = Time.getSystemTime();
	}

	public final String getCommand()
	{
		return getClass().getAnnotation(SerializerId2.class).value();
	}

	public final Direction getDirection()
	{
		return this.direction;
	}

	public final void setDirection(Direction direction)
	{
		this.direction = direction;
	}

	public final long getMagic() {
		return this.magic;
	}

	public final int getSize()
	{
		return this.size;
	}

	public final long getSeq()
	{
		return this.seq;
	}

	public long getTimestamp()
	{
		return this.timestamp;
	}

	public ECPublicKey getSender()
	{
		return this.sender;
	}

	// SIGNABLE //
	public final ECSignature getSignature()
	{
		return this.signature;
	}

	public final void setSignature(ECSignature signature)
	{
		this.signature = signature;
	}

	public final void sign(ECKeyPair key) throws CryptoException
	{
		if (this.signature != null)
			throw new IllegalStateException("Message "+this+" is already signed");
		
		this.sender = key.getPublicKey();
		this.signature = (ECSignature) key.sign(getHash());
	}

	public final boolean verify(ECPublicKey key) throws CryptoException 
	{
		return key.verify(getHash(), this.signature);
	}

	private byte[] toByteArray() throws IOException
	{
		if (this.bytes != null)
			return this.bytes;
		
		byte[] bytes = Serialization.getInstance().toDson(this, Output.WIRE);
		bytes = Snappy.compress(bytes);
		ByteArrayOutputStream baos = new ByteArrayOutputStream(bytes.length+5);
		DataOutputStream dos = new DataOutputStream(baos);
		dos.writeBoolean(true);
		dos.writeInt(bytes.length);
		dos.write(bytes);
		
		this.size = baos.size();
		this.bytes = baos.toByteArray();
		return this.bytes;
	}

	@Override
	public String toString()
	{
		return getCommand()+":"+getDirection()+":"+getHash()+" @ "+getTimestamp();
	}
}