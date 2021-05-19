package org.fuserleer;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

import org.fuserleer.events.EventListener;
import org.fuserleer.ledger.PendingAtom;
import org.fuserleer.ledger.StateLockedException;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.Particle;
import org.fuserleer.ledger.events.AtomPositiveCommitEvent;
import org.fuserleer.ledger.events.AtomCommitTimeoutEvent;
import org.fuserleer.ledger.events.AtomAcceptedTimeoutEvent;
import org.fuserleer.ledger.events.AtomExceptionEvent;
import org.fuserleer.ledger.events.AtomNegativeCommitEvent;
import org.fuserleer.ledger.events.AtomUnpreparedTimeoutEvent;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializationException;
import org.java_websocket.WebSocket;
import org.java_websocket.WebSocketImpl;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.json.JSONArray;
import org.json.JSONException;
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
		super(new InetSocketAddress(context.getConfiguration().get("websocket.address", "0.0.0.0"), context.getConfiguration().get("websocket.port", WebSocketImpl.DEFAULT_PORT)),
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
				JSONArray particles = new JSONArray();
				for (Particle particle : atom.getParticles())
					particles.put(Serialization.getInstance().toJsonObject(particle, Output.API));
				eventJSON.put("particles", particles);
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
		public void on(AtomPositiveCommitEvent event) throws JSONException, SerializationException 
		{
			JSONObject eventJSON = new JSONObject();
			eventJSON.put("type", "committed");
			eventJSON.put("atom", event.getAtom().getHash());
			eventJSON.put("certificate", Serialization.getInstance().toJsonObject(event.getPendingAtom().getCertificate(), Output.API));
			JSONArray particles = new JSONArray();
			for (Particle particle : event.getAtom().getParticles())
				particles.put(Serialization.getInstance().toJsonObject(particle, Output.API));
			eventJSON.put("particles", particles);
			JSONArray automata = new JSONArray();
			for (Particle particle : event.getAtom().getAutomata())
				automata.put(Serialization.getInstance().toJsonObject(particle, Output.API));
			eventJSON.put("automata", automata);
			WebSocketService.this.broadcast(eventJSON.toString());
		}

		@Subscribe
		public void on(AtomNegativeCommitEvent event) throws SerializationException
		{
			JSONObject eventJSON = new JSONObject();
			eventJSON.put("type", "rejected");
			eventJSON.put("atom", event.getAtom().getHash());
			eventJSON.put("certificate", Serialization.getInstance().toJsonObject(event.getPendingAtom().getCertificate(), Output.API));
			JSONArray particles = new JSONArray();
			for (Particle particle : event.getAtom().getParticles())
				particles.put(Serialization.getInstance().toJsonObject(particle, Output.API));
			eventJSON.put("particles", particles);
			JSONArray automata = new JSONArray();
			for (Particle particle : event.getAtom().getAutomata())
				automata.put(Serialization.getInstance().toJsonObject(particle, Output.API));
			eventJSON.put("automata", automata);
			WebSocketService.this.broadcast(eventJSON.toString());
		}
		
		@Subscribe
		public void on(AtomExceptionEvent event) throws SerializationException
		{
			if (event.getException() instanceof StateLockedException)
				return;
			
			JSONObject eventJSON = new JSONObject();
			eventJSON.put("type", "exception");
			eventJSON.put("atom", event.getAtom().getHash());
			JSONArray particles = new JSONArray();
			for (Particle particle : event.getAtom().getParticles())
				particles.put(Serialization.getInstance().toJsonObject(particle, Output.API));
			eventJSON.put("particles", particles);
			JSONArray automata = new JSONArray();
			for (Particle particle : event.getAtom().getAutomata())
				automata.put(Serialization.getInstance().toJsonObject(particle, Output.API));
			eventJSON.put("automata", automata);
			eventJSON.put("error", event.getException().toString());
			WebSocketService.this.broadcast(eventJSON.toString());
		}
		
		@Subscribe
		public void on(AtomUnpreparedTimeoutEvent event) throws SerializationException
		{
			timedout(event.getPendingAtom());
		}

		@Subscribe
		public void on(AtomAcceptedTimeoutEvent event) throws SerializationException 
		{
			timedout(event.getPendingAtom());
		}

		@Subscribe
		public void on(AtomCommitTimeoutEvent event) throws SerializationException
		{
			timedout(event.getPendingAtom());
		}
		
		private void timedout(PendingAtom pendingAtom) throws SerializationException
		{
			JSONObject eventJSON = new JSONObject();
			eventJSON.put("type", "timeout");
			eventJSON.put("atom", pendingAtom.getHash());
			if (pendingAtom.getAtom() != null)
			{
				JSONArray particles = new JSONArray();
				for (Particle particle : pendingAtom.getAtom().getParticles())
					particles.put(Serialization.getInstance().toJsonObject(particle, Output.API));
				eventJSON.put("particles", particles);
				JSONArray automata = new JSONArray();
				for (Particle particle : pendingAtom.getAtom().getAutomata())
					automata.put(Serialization.getInstance().toJsonObject(particle, Output.API));
				eventJSON.put("automata", automata);
			}
			WebSocketService.this.broadcast(eventJSON.toString());
		}
	};
}
