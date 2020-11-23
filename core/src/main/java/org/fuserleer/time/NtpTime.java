package org.fuserleer.time;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Objects;

import org.fuserleer.Configuration;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;

public class NtpTime implements TimeProvider
{
	private static final Logger log = Logging.getLogger();

	private String 		server = null;
	private double 		roundTripDelay = 0;
	private	double 		localClockOffset = 0;

	private int			attempts = 0;
	private int			offset = 0;

	public NtpTime(Configuration configuration)
	{
		this.server = Objects.requireNonNull(configuration.get("ntp.pool"));
		this.roundTripDelay = 0;
		this.localClockOffset = 0;

		// do the sync //
		boolean success = false;

		while (this.attempts < 3 && !success)
		{
			DatagramSocket socket = null;

			try
			{
				// Send request
				socket = new DatagramSocket();
				socket.setSoTimeout(5000);

				InetAddress address = InetAddress.getByName(this.server);
				byte[] buf = new NtpMessage().toByteArray();
				DatagramPacket packet =	new DatagramPacket(buf, buf.length, address, 123);

				// Set the transmit timestamp *just* before sending the packet
				// ToDo: Does this actually improve performance or not?
				NtpMessage.encodeTimestamp(packet.getData(), 40, (System.currentTimeMillis()/1000.0) + 2208988800.0);

				socket.send(packet);

				// Get response
				log.info("NTP request sent, waiting for response...\n");
				packet = new DatagramPacket(buf, buf.length);
				socket.receive(packet);

				// Immediately record the incoming timestamp
				double destinationTimestamp = (System.currentTimeMillis()/1000.0) + 2208988800.0;

				// Process response
				NtpMessage msg = new NtpMessage(packet.getData());

				// Corrected, according to RFC2030 errata
				this.roundTripDelay = (destinationTimestamp-msg.originateTimestamp) - (msg.transmitTimestamp-msg.receiveTimestamp);
				this.localClockOffset = ((msg.receiveTimestamp - msg.originateTimestamp) + (msg.transmitTimestamp - destinationTimestamp)) / 2;

				log.info(msg.toString());
				success = true;
			}
			catch (Exception ex)
			{
				if (this.attempts >= 3)
					throw new RuntimeException(ex);
			}
			finally
			{
				this.attempts++;

				if (socket != null)
					socket.close();
			}
		}

		if (!success)
			throw new RuntimeException("Unable to start NTP service using "+server);
	}

	@Override
	public boolean isSynchronized()
	{
		return this.server == null?false:true;
	}

	/**
	 * Returns the offset in seconds set in this NtpService
	 *
	 * @return
	 */
	private int getOffset()
	{
		return this.offset;
	}

	/**
	 * Sets the offset in seconds for this NtpService
	 *
	 * @param offset
	 */
	private void setOffset(int offset)
	{
		this.offset = offset;
	}

	/**
	 * Returns a corrected System time
	 *
	 * @return
	 */
	@Override
	public long getSystemTime()
	{
		return (long) (System.currentTimeMillis()+(this.localClockOffset*1000.0));
	}

	/**
	 * Returns a corrected UTC time in seconds
	 *
	 * @return
	 */
	@Override
	public synchronized int getLedgerTimeSeconds()
	{
		return (int) (getLedgerTimeMS() / 1000L);
	}

	/**
	 * Returns a corrected UTC time in milliseconds
	 *
	 * @return
	 */
	@Override
	public synchronized long getLedgerTimeMS()
	{
		return (long) (System.currentTimeMillis() + (this.localClockOffset * 1000.0) + (this.roundTripDelay * 1000.0) + (this.offset * 1000L));
	}
}
