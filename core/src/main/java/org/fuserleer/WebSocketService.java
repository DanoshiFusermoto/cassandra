package org.fuserleer;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

import org.fuserleer.events.EventListener;
import org.fuserleer.ledger.StateLockedException;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.events.AtomAcceptedEvent;
import org.fuserleer.ledger.events.AtomCommitTimeoutEvent;
import org.fuserleer.ledger.events.AtomDiscardedEvent;
import org.fuserleer.ledger.events.AtomExceptionEvent;
import org.fuserleer.ledger.events.AtomRejectedEvent;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.serialization.Serialization;
import org.java_websocket.WebSocket;
import org.java_websocket.WebSocketImpl;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.common.eventbus.Subscribe;

/** 
 * Simple Websocket service that clients can connect to and receive a stream of JSON formatted events regarding atoms.
 * 
 * Needs lots of additional functionality to be a real Websocket service.  Currently used to assist with testing, hence simplicity.
 * 
 * @author Dan
 *
 */
public final class WebSocketService extends WebSocketServer
{
	private static final Logger websocketLog = Logging.getLogger("websocket");

	private final Context context;
	
	public WebSocketService(final Context context)
	{
		super(new InetSocketAddress(context.getConfiguration().get("websocket.address", "127.0.0.1"), context.getConfiguration().get("websocket.port", WebSocketImpl.DEFAULT_PORT)),
			  context.getConfiguration().get("websocket.decoders", Math.max(4, Runtime.getRuntime().availableProcessors())));
		
		this.context = Objects.requireNonNull(context, "Context is null");
	}
	
	@Override
	public void onStart()
	{
		this.context.getEvents().register(this.asyncAtomListener);
	}

	@Override
	public void stop(int timeout) throws InterruptedException
	{
		this.context.getEvents().unregister(this.asyncAtomListener);
		super.stop(timeout);
	}
	
	@Override
	public void onOpen(WebSocket conn, ClientHandshake handshake)
	{
	}

	@Override
	public void onClose(WebSocket conn, int code, String reason, boolean remote)
	{
	}

	@Override
	public void onMessage(WebSocket conn, String message)
	{
		JSONObject messageJSON = new JSONObject(message);
		String type = messageJSON.getString("type");
		if (type.equalsIgnoreCase("submit") == true)
		{
			JSONObject atomJSON = null;
			Atom atom = null;
			try
			{
				atomJSON = messageJSON.getJSONObject("atom");
				atom = Serialization.getInstance().fromJsonObject(atomJSON, Atom.class);
			}
			catch (Exception ex)
			{
				JSONObject eventJSON = new JSONObject();
				eventJSON.put("type", "exception");
				eventJSON.put("error", "Atom was malformed with exception "+ex.toString());
				conn.send(eventJSON.toString());
				websocketLog.error(this.context.getName()+": Submission of atom "+atom.getHash()+" failed from "+conn.getRemoteSocketAddress(), ex);
				return;
			}
			
			try
			{
				if (this.context.getLedger().submit(atom) == false)
					new RejectedExecutionException("Submission of atom "+atom.getHash()+" failed");
			}
			catch (Exception ex)
			{
				JSONObject eventJSON = new JSONObject();
				eventJSON.put("type", "exception");
				eventJSON.put("atom", atom.getHash());
				eventJSON.put("particles", new JSONArray(atom.getParticles().stream().map(p -> p.getHash()).collect(Collectors.toList())));
				eventJSON.put("error", ex.toString());
				conn.send(eventJSON.toString());
				websocketLog.error(this.context.getName()+": Submission of atom "+atom.getHash()+" failed from "+conn.getRemoteSocketAddress(), ex);
			}
		}
	}

	@Override
	public void onError(WebSocket conn, Exception ex)
	{
	}

	// ASYNC ATOM LISTENER //
	private EventListener asyncAtomListener = new EventListener()
	{
		@Subscribe
		public void on(AtomAcceptedEvent event) 
		{
			JSONObject eventJSON = new JSONObject();
			eventJSON.put("type", "accepted");
			eventJSON.put("atom", event.getAtom().getHash());
			eventJSON.put("certificate", event.getPendingAtom().getCertificate().getHash());
			eventJSON.put("particles", new JSONArray(event.getAtom().getParticles().stream().map(p -> p.getHash()).collect(Collectors.toList())));
			WebSocketService.this.broadcast(eventJSON.toString());
		}

		@Subscribe
		public void on(AtomRejectedEvent event)
		{
			JSONObject eventJSON = new JSONObject();
			eventJSON.put("type", "rejected");
			eventJSON.put("atom", event.getAtom().getHash());
			eventJSON.put("certificate", event.getPendingAtom().getCertificate().getHash());
			eventJSON.put("particles", new JSONArray(event.getAtom().getParticles().stream().map(p -> p.getHash()).collect(Collectors.toList())));
			WebSocketService.this.broadcast(eventJSON.toString());
		}
		
		@Subscribe
		public void on(AtomExceptionEvent event)
		{
			if (event.getException() instanceof StateLockedException)
				return;
			
			JSONObject eventJSON = new JSONObject();
			eventJSON.put("type", "exception");
			eventJSON.put("atom", event.getAtom().getHash());
			eventJSON.put("particles", new JSONArray(event.getAtom().getParticles().stream().map(p -> p.getHash()).collect(Collectors.toList())));
			eventJSON.put("error", event.getException().toString());
			WebSocketService.this.broadcast(eventJSON.toString());
		}
		
		@Subscribe
		public void on(AtomDiscardedEvent event) 
		{
			JSONObject eventJSON = new JSONObject();
			eventJSON.put("type", "discarded");
			eventJSON.put("atom", event.getAtom().getHash());
			eventJSON.put("particles", new JSONArray(event.getAtom().getParticles().stream().map(p -> p.getHash()).collect(Collectors.toList())));
			WebSocketService.this.broadcast(eventJSON.toString());
		}

		@Subscribe
		public void on(AtomCommitTimeoutEvent event)
		{
			JSONObject eventJSON = new JSONObject();
			eventJSON.put("type", "timeout");
			eventJSON.put("atom", event.getAtom().getHash());
			eventJSON.put("particles", new JSONArray(event.getAtom().getParticles().stream().map(p -> p.getHash()).collect(Collectors.toList())));
			WebSocketService.this.broadcast(eventJSON.toString());
		}
	};
}
