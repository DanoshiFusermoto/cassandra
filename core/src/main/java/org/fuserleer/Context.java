package org.fuserleer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import org.fuserleer.crypto.CryptoException;
import org.fuserleer.database.DatabaseEnvironment;
import org.fuserleer.database.SystemMetaData;
import org.fuserleer.events.Events;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.executors.Executor;
import org.fuserleer.executors.ScheduledExecutable;
import org.fuserleer.ledger.Ledger;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.Network;
import org.fuserleer.node.LocalNode;
import org.fuserleer.node.Node;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.Serialization;
import org.java_websocket.WebSocketImpl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

public final class Context implements Service
{
	private static final Logger log = Logging.getLogger ();
	
	private static final Map<String, Context> contexts = Collections.synchronizedMap(new HashMap<String, Context>());
	private static final AtomicInteger incrementer = new AtomicInteger(0);
	private static Context _default = null;
	
	public final static Context create() throws StartupException
	{
		return Context.create("node-"+contexts.size(), Configuration.getDefault());
	}

	public final static Context createCleanAndStart() throws StartupException
	{
		try
		{
			Context context = Context.create();
			context.clean();
			context.start();
			return context;
		}
		catch (IOException ioex)
		{
			throw new StartupException(ioex);
		}
	}

	public final static Context create(final Configuration config) throws StartupException
	{
		Objects.requireNonNull(config);
		
		return Context.create("node-"+contexts.size(), config);
	}

	public final static Context createAndStart() throws StartupException
	{
		return createAndStart("node-"+contexts.size(), Configuration.getDefault());
	}

	public final static Context createAndStart(final Configuration config) throws StartupException
	{
		return createAndStart("node-"+contexts.size(), config);
	}

	public final static Context createAndStart(final String name, final Configuration config) throws StartupException
	{
		Context context = create(name.toLowerCase(), config);
		context.start();
		return context;
	}
	
	public final static Collection<Context> createAndStart(final int count, final String name, final Configuration configuration) throws StartupException
	{
		List<Context> contexts = new ArrayList<Context>();
		try
		{
			for (int cc = 0 ; cc < count ; cc++)
			{
				Context context = Context.create(name.toLowerCase()+"-"+incrementer.get(), configuration);
				context.start();
				contexts.add(context);
			}
			
			return contexts;
		}
		catch (StartupException stex)
		{
			for (Context context : contexts)
			{
				try
				{
					context.stop();
				}
				catch (TerminationException tex)
				{
					log.fatal(tex);
				}
			}
			
			throw stex;
		}
	}
	
	public final static Context create(final String name, final Configuration configuration) throws StartupException
	{
		Objects.requireNonNull(name);
		Objects.requireNonNull(configuration);
		
		synchronized(contexts)
		{
			if (contexts.containsKey(name.toLowerCase()) == true)
				throw new IllegalStateException("Context "+name.toLowerCase()+" already created");
			
			Configuration contextConfig = new Configuration(Objects.requireNonNull(configuration));
			contextConfig.set("network.port", configuration.get("network.port", Universe.getDefault().getPort())+incrementer.get());
			contextConfig.set("network.udp", configuration.get("network.udp", Universe.getDefault().getPort())+incrementer.get());
			contextConfig.set("websocket.port", configuration.get("websocket.port", WebSocketImpl.DEFAULT_PORT)+incrementer.get());
			
			if (incrementer.get() > 0)
			{
				String seeds[] = configuration.get("network.seeds", "").split(",");
				Set<String> seedSet = new HashSet<String>();
				for (String seed : seeds)
					seedSet.add(seed);
				seedSet.add(contextConfig.get("network.address", "127.0.0.1"));
				contextConfig.set("network.seeds", String.join(",", seedSet));
			}

			LocalNode node;
			try
			{
				node = LocalNode.load(name.toLowerCase(), contextConfig, true);
			}
			catch (CryptoException e)
			{
				throw new StartupException(e);
			}
			
			for (Context context : contexts.values())
				if (context.node.getIdentity().equals(node.getIdentity()) == true)
					throw new IllegalStateException("Context "+name.toLowerCase()+":"+node+" already created");
			
			Context context = new Context(name.toLowerCase(), node, contextConfig);
			Context.incrementer.incrementAndGet();
			contexts.put(name.toLowerCase(), context);
			
			if (Context._default == null)
				Context._default = context;
			
			return context;
		}
	}

	public final static void stop(final Context context) throws TerminationException
	{
		Objects.requireNonNull(context).stop();
		
		contexts.remove(context.getName());
		if (Context._default == context)
			Context._default = null;
	}

	public final static void stopAll() throws TerminationException
	{
		synchronized(contexts)
		{
			for (Context context : contexts.values())
				Objects.requireNonNull(context).stop();
		
			contexts.clear();
			Context._default = null;
		}
	}

	public final static void setDefault(final Context context)
	{
		Objects.requireNonNull(context);
		Context._default = context;
	}

	public final static Context get()
	{
		return Context._default;
	}

	public final static Context get(String name)
	{
		return contexts.get(name.toLowerCase());
	}
	
	public final static Collection<Context> getAll()
	{
		return Collections.unmodifiableCollection(new ArrayList<Context>(contexts.values()));
	}
	
	public static void clear()
	{
		contexts.clear();
	}
	
	// FIXME: This is a pretty horrible way of ensuring unit tests are stable,
	// as freeMemory() can return varying numbers between calls.
	// This is adjusted via reflection in the unit tests to be something that
	// returns a constant.
	private static LongSupplier freeMemory = () -> Runtime.getRuntime().freeMemory();
	private static LongSupplier maxMemory = () -> Runtime.getRuntime().maxMemory();
	private static LongSupplier totalMemory = () -> Runtime.getRuntime().totalMemory();

	private final String 			name;
	private final LocalNode			node;
	private final Configuration		configuration;
	private final Events			events;
	private final Network			network;
	private final Ledger			ledger;
	private final SystemMetaData 	metaData;
	private final DatabaseEnvironment environment;
	
	private transient Future<?> metaDataTaskFuture;

	/** A null Context used for unit testing */
	private Context(Configuration configuration)
	{
		this.name = "null";
		this.configuration = Objects.requireNonNull(configuration);
		this.node = null;
		this.environment = null;
		this.events = null;
		this.metaData = null;
		this.network = null;
		this.ledger = null;
	}
	
	public Context(String name, LocalNode node, Configuration configuration)
	{
		this.name = Objects.requireNonNull(name).toLowerCase();
		this.configuration = Objects.requireNonNull(configuration);
		this.node = Objects.requireNonNull(node);
		this.environment = new DatabaseEnvironment(this, new File(System.getProperty("user.dir")+File.separatorChar+"database"+File.separatorChar+name));
		this.events = new Events(this);
		this.metaData = new SystemMetaData(this);
		this.network = new Network(this);
		this.ledger = new Ledger(this);
	}
	
	@Override
	public void start() throws StartupException
	{
		this.events.start();
		this.metaData.start();		
		if (this.metaData.has("node.local") == true) 
		{
			try 
			{
				byte[] nodeBytes = this.metaData.get("node.local", (byte[]) null);
				if (nodeBytes == null)
					throw new IllegalStateException("Expected node.local bytes but got null");
				
				Node persisted = Serialization.getInstance().fromDson(nodeBytes, Node.class);

				if (persisted.getIdentity().equals(Context.this.node.getIdentity()) == false) // TODO what happens if has changed?  Dump everything?
					log.warn("Node key has changed from "+persisted.getIdentity()+" to "+Context.this.node.getIdentity());
				
				Context.this.node.fromPersisted(persisted);
			} 
			catch (IOException ex) 
			{
				log.error("Could not load persisted system state from SystemMetaData", ex);
			}
		}
		
		this.network.start();
		this.ledger.start();
		
		this.node.setHead(this.ledger.getHead());
		this.metaDataTaskFuture = Executor.getInstance().scheduleAtFixedRate(new ScheduledExecutable(1, 1, TimeUnit.SECONDS)
		{
			@Override
			public void execute()
			{
				try 
				{
					byte[] nodeBytes = Serialization.getInstance().toDson(Context.this.getNode(), Output.PERSIST);
					Context.this.metaData.put("node.local", nodeBytes);
				} 
				catch (IOException e) 
				{
					log.error("Could not persist local node state", e);
				}
			}
		});
	}
	
	@Override
	public void stop() throws TerminationException
	{
		this.ledger.stop();
		this.network.stop();
		this.metaDataTaskFuture.cancel(false);
		this.metaData.stop();
		this.events.stop();
	}
	
	public void clean() throws IOException
	{
		this.ledger.clean();
		this.network.clean();
		this.metaData.clean();
	}

	public Configuration getConfiguration()
	{
		return this.configuration;
	}

	public Network getNetwork()
	{
		return this.network;
	}
	
	public Events getEvents()
	{
		return this.events;
	}

	public LocalNode getNode()
	{
		return this.node;
	}

	public Ledger getLedger()
	{
		return this.ledger;
	}

	public SystemMetaData getMetaData()
	{
		return this.metaData;
	}

	public DatabaseEnvironment getDatabaseEnvironment()
	{
		return this.environment;
	}
	
	// Property "ledger" - 1 getter
	// No really obvious way of doing this better
	@JsonProperty("ledger")
	@DsonOutput(Output.API)
	Map<String, Object> getJsonLedger() 
	{
		SystemMetaData smd = this.metaData;

		Map<String, Object> latency = ImmutableMap.of(
			"path", smd.get("ledger.latency.path", 0),
			"persist", smd.get("ledger.latency.persist", 0)
		);

		Map<String, Object> faults = ImmutableMap.of(
			"tears", smd.get("ledger.faults.tears", 0),
			"assists", smd.get("ledger.faults.assists", 0),
			"stitched", smd.get("ledger.faults.stitched", 0),
			"failed", smd.get("ledger.faults.failed", 0)
		);

		return ImmutableMap.<String, Object>builder().
			put("processed", smd.get("ledger.processed", 0)).
			put("processing", smd.get("ledger.processing", 0)).
			put("stored", smd.get("ledger.stored", 0)).
			put("storedPerShard", smd.get("ledger.storedPerShard", "0")).
			put("storing", smd.get("ledger.storing", 0)).
			put("storingPerShard", smd.get("ledger.storingPerShard", 0)).
			put("storing.peak", smd.get("ledger.storing.peak", 0)).
			put("checksum", smd.get("ledger.checksum", 0)).
			put("latency", latency).
			put("faults", faults).build();
	}

	// Property "global" - 1 getter
	@JsonProperty("global")
	@DsonOutput(Output.API)
	Map<String, Object> getJsonGlobal() 
	{
		return ImmutableMap.of(
			"stored", this.metaData.get("ledger.network.stored", 0),
			"processing", this.metaData.get("ledger.network.processing", 0),
			"storing", this.metaData.get("ledger.network.storing", 0)
		);
	}

	// Property "events" - 1 getter
	@JsonProperty("events")
	@DsonOutput(Output.API)
	Map<String, Object> getJsonEvents() 
	{
		SystemMetaData smd = this.metaData;

		Map<String, Object> processed = ImmutableMap.of(
			"synchronous", smd.get("events.processed.synchronous", 0L),
			"asynchronous", smd.get("events.processed.asynchronous", 0L)
		);

		return ImmutableMap.of(
			"processed", processed,
			"processing", smd.get("events.processing", 0L),
			"broadcast",  smd.get("events.broadcast", 0L),
			"queued", smd.get("events.queued", 0L),
			"dequeued", smd.get("events.dequeued", 0L)
		);
	}

	// Property "messages" - 1 getter
	// No obvious improvements here
	@JsonProperty("messages")
	@DsonOutput(Output.API)
	Map<String, Object> getJsonMessages() {
		Map<String, Object> outbound = ImmutableMap.of(
				"sent", this.metaData.get("messages.outbound.sent", 0),
				"processed", this.metaData.get("messages.outbound.processed", 0),
				"pending", this.metaData.get("messages.outbound.pending", 0),
				"aborted", this.metaData.get("messages.outbound.aborted", 0));
		Map<String, Object> inbound = ImmutableMap.of(
				"processed", this.metaData.get("messages.inbound.processed", 0),
				"received", this.metaData.get("messages.inbound.received", 0),
				"pending", this.metaData.get("messages.inbound.pending", 0),
				"discarded", this.metaData.get("messages.inbound.discarded", 0));
		return ImmutableMap.of(
				"inbound", inbound,
				"outbound", outbound);
	}

	// Property "memory" - 1 getter
	// No obvious improvements here
	@JsonProperty("memory")
	@DsonOutput(Output.API)
	Map<String, Object> getJsonMemory() 
	{
		return ImmutableMap.of(
				"free", freeMemory.getAsLong(),
				"total", totalMemory.getAsLong(),
				"max", maxMemory.getAsLong());
	}

	// Property "processors" - 1 getter
	// No obvious improvements here
	@JsonProperty("processors")
	@DsonOutput(Output.API)
	int getJsonProcessors() 
	{
		return Runtime.getRuntime().availableProcessors();
	}
	
	@Override
	public final String getName()
	{
		return this.name;
	}
	
	@Override
	public String toString()
	{
		return this.name+":"+this.node.getIdentity();
	}
}
