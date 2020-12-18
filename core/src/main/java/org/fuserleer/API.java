package org.fuserleer;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.fuserleer.common.Agent;
import org.fuserleer.common.Match;
import org.fuserleer.common.Order;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Hash;
import org.fuserleer.database.Indexable;
import org.fuserleer.database.IndexablePrimitive;
import org.fuserleer.database.Identifier;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.ledger.Block;
import org.fuserleer.ledger.BlockHeader;
import org.fuserleer.ledger.SearchQuery;
import org.fuserleer.ledger.SearchResponse;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.Particle;
import org.fuserleer.ledger.atoms.Particle.Spin;
import org.fuserleer.ledger.atoms.UniqueParticle;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.Protocol;
import org.fuserleer.network.peers.Peer;
import org.fuserleer.network.peers.filters.StandardPeerFilter;
import org.fuserleer.node.Node;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.mapper.JacksonCodecConstants;
import org.fuserleer.utils.Bytes;
import org.fuserleer.utils.UInt256;
import org.json.JSONArray;
import org.json.JSONObject;

// TODO upgrade allowing multiple API instances for each context.  Needs multiple API endpoints on different ports. 
public class API implements Service
{
	private static final Logger apiLog = Logging.getLogger("api");
	
	public final static int DEFAULT_PORT = 8080;
	public final static String DEFAULT_API_PATH = "/api";
	public final static String DEFAULT_SCHEME = "http://";

	private static API instance;
	
	public synchronized static API create(Context context)
	{
		if (instance != null)
			throw new RuntimeException("API instance already created");
		
		instance = new API(context);
		return instance;
	}

	public synchronized static API getInstance()
	{
		if (instance == null)
			throw new RuntimeException("API instance not created");
		
		return instance;
	}
	
	private final Context context;
	
	private API(Context context)
	{
		this.context = Objects.requireNonNull(context);
	}
	
	@Override
	public void start() throws StartupException
	{
		try
		{
			spark.Spark.port(this.context.getConfiguration().get("api.port", DEFAULT_PORT));
			
			spark.Spark.options("/*", (request, response) -> 
			{
	            String accessControlRequestHeaders = request.headers("Access-Control-Request-Headers");
	            if (accessControlRequestHeaders != null) 
	            	response.header("Access-Control-Allow-Headers", accessControlRequestHeaders);

	            String accessControlRequestMethod = request.headers("Access-Control-Request-Method");
	            if (accessControlRequestMethod != null)
	            	response.header("Access-Control-Allow-Methods", accessControlRequestMethod);

	            return "OK";
	        });

			spark.Spark.before((request, response) -> response.header("Access-Control-Allow-Origin", "*"));
			
			spark.Spark.get(this.context.getConfiguration().get("api.url", DEFAULT_API_PATH)+"/ledger", (req, res) -> 
			{
				JSONObject responseJSON = new JSONObject();
				
				try
				{
					BlockHeader   head = API.this.context.getLedger().getHead();
					responseJSON.put("head", Serialization.getInstance().toJsonObject(head, Output.ALL));

					JSONObject statistics = new JSONObject();
					
					JSONObject processed = new JSONObject();
					processed.put("atoms", API.this.context.getMetaData().get("ledger.processed.atoms", 0l));
					processed.put("particles", API.this.context.getMetaData().get("ledger.processed.particles", 0l));
					statistics.put("processed", processed);

					JSONObject throughput = new JSONObject();
					throughput.put("atoms", API.this.context.getMetaData().get("ledger.throughput.atoms", 0l));
					throughput.put("particles", API.this.context.getMetaData().get("ledger.throughput.particles", 0l));
					statistics.put("throughput", throughput);

					statistics.put("finality", API.this.context.getMetaData().get("ledger.commit.latency", 0l));
					responseJSON.put("statistics", statistics);
					
					status(responseJSON, 200);
				}
				catch(Throwable t)
				{
					status(responseJSON, 500, t.toString());
					apiLog.error(t);
				}

				return responseJSON.toString(4);
			});
			
			spark.Spark.get(this.context.getConfiguration().get("api.url", DEFAULT_API_PATH)+"/ledger/recent", (req, res) -> 
			{
				JSONObject responseJSON = new JSONObject();
				
				try
				{
					int limit = Integer.parseUnsignedInt(req.queryParamOrDefault("limit", "100"));
					if (limit > 100)
						throw new IllegalArgumentException("Limit is greater than 100");

					Block block = API.this.context.getLedger().get(API.this.context.getLedger().getHead().getHash(), Block.class);
					long offset = block.getHeader().getIndex()+block.getHeader().getInventory().size();
					long nextOffset = offset;
					JSONArray headersArray = new JSONArray();
					JSONArray atomsArray = new JSONArray();
					
					while(block != null && atomsArray.length() < limit)
					{
						headersArray.put(Serialization.getInstance().toJsonObject(block.getHeader(), Output.ALL));
						Iterator<Atom> atomIterator = block.getAtoms().descendingIterator();
						while(atomIterator.hasNext() == true)
						{
							Atom atom = atomIterator.next();
							atomsArray.put(Serialization.getInstance().toJsonObject(atom, Output.ALL));
							nextOffset--;
						}
						
						block = API.this.context.getLedger().get(block.getHeader().getPrevious(), Block.class);
					}
					
					responseJSON.put("atoms", atomsArray);
					responseJSON.put("headers", headersArray);
					status(responseJSON, 200, offset, nextOffset, limit, nextOffset == offset);
				}
				catch(Throwable t)
				{
					status(responseJSON, 500, t.toString());
					apiLog.error(t);
				}
				
				return responseJSON.toString(4);
			});

			spark.Spark.get(this.context.getConfiguration().get("api.url", DEFAULT_API_PATH)+"/ledger/recent/:container", (req, res) -> 
			{
				JSONObject responseJSON = new JSONObject();
				
				try
				{
					int limit = Integer.parseUnsignedInt(req.queryParamOrDefault("limit", "100"));
					if (limit > 100)
						throw new IllegalArgumentException("Limit is greater than 100");

					@SuppressWarnings("unchecked")
					Class<? extends Particle> container = (Class<? extends Particle>) Serialization.getInstance().getClassForId(req.params("container").toLowerCase());
					Block block = API.this.context.getLedger().get(API.this.context.getLedger().getHead().getHash(), Block.class);
					long offset = block.getHeader().getIndex()+block.getHeader().getInventory().size();
					long nextOffset = offset;
					JSONArray headersArray = new JSONArray();
					JSONArray atomsArray = new JSONArray();
					
					while(block != null && atomsArray.length() < limit)
					{
						boolean addedHeader = false;
						Iterator<Atom> atomIterator = block.getAtoms().descendingIterator();
						while(atomIterator.hasNext() == true)
						{
							Atom atom = atomIterator.next();
							int numMatches = atom.getParticles(container).size();
							if (numMatches > 0)
							{
								if (addedHeader == false)
									headersArray.put(Serialization.getInstance().toJsonObject(block.getHeader(), Output.ALL));
								atomsArray.put(Serialization.getInstance().toJsonObject(atom, Output.ALL));
							}
							
							if (atomsArray.length() == limit)
								break;

							nextOffset--;
						}

						block = API.this.context.getLedger().get(block.getHeader().getPrevious(), Block.class);
					}
					
					responseJSON.put("atoms", atomsArray);
					responseJSON.put("headers", headersArray);
					status(responseJSON, 200, offset, nextOffset, limit, nextOffset == offset);
					status(responseJSON, 200, offset, nextOffset, limit, nextOffset == -1);
				}
				catch(Throwable t)
				{
					status(responseJSON, 500, t.toString());
					apiLog.error(t);
				}
				
				return responseJSON.toString(4);
			});

			spark.Spark.get(this.context.getConfiguration().get("api.url", DEFAULT_API_PATH)+"/ledger/atom/:hash", (req, res) -> 
			{
				JSONObject responseJSON = new JSONObject();
				
				try
				{
					Atom atom = API.this.context.getLedger().get(Indexable.from(req.params("hash"), Atom.class), Atom.class);
					if (atom == null)
						status(responseJSON, 404, "Atom "+req.params("hash")+" not found");
					else
					{
						responseJSON.put("action", Serialization.getInstance().toJsonObject(atom, Output.ALL));
						status(responseJSON, 200);
					}
				}
				catch(Throwable t)
				{
					status(responseJSON, 500, t.toString());
					apiLog.error(t);
				}
				
				return responseJSON.toString(4);
			});
			
			spark.Spark.get(this.context.getConfiguration().get("api.url", DEFAULT_API_PATH)+"/ledger/particle/:hash", (req, res) -> 
			{
				JSONObject responseJSON = new JSONObject();
				
				try
				{
					Atom atom = API.this.context.getLedger().get(Indexable.from(req.params("hash"), Particle.class), Atom.class);
					if (atom == null)
						status(responseJSON, 404, "Particle "+req.params("hash")+" not found");
					else
					{
						responseJSON.put("action", Serialization.getInstance().toJsonObject(atom, Output.ALL));
						status(responseJSON, 200);
					}
				}
				catch(Throwable t)
				{
					status(responseJSON, 500, t.toString());
					apiLog.error(t);
				}
				
				return responseJSON.toString(4);
			});
			
			spark.Spark.get(this.context.getConfiguration().get("api.url", DEFAULT_API_PATH)+"/ledger/indexable/:container/:key", (req, res) -> 
			{
				JSONObject responseJSON = new JSONObject();
				
				try
				{
					Class<?> container = Serialization.getInstance().getClassForId(req.params("container").toLowerCase());
					if (container == null)
						status(responseJSON, 404, "Container "+req.params("container").toLowerCase()+" not found");
					else if (IndexablePrimitive.class.isAssignableFrom(container) == false)
						status(responseJSON, 500, "Container "+req.params("container").toLowerCase()+" is not a indexable");
					else
					{
						Atom atom = API.this.context.getLedger().get(Indexable.from(decodeQueryValue(req.params("key")), (Class<? extends IndexablePrimitive>) container), Atom.class);
						if (atom == null)
							status(responseJSON, 404, "Indexable "+req.params("container").toLowerCase()+":"+req.params("key")+" not found");
						else
						{
							responseJSON.put("atom", Serialization.getInstance().toJsonObject(atom, Output.ALL));
							status(responseJSON, 200);
						}
					}
				}
				catch(Throwable t)
				{
					status(responseJSON, 500, t.toString());
					apiLog.error(t);
				}
				
				return responseJSON.toString(4);
			});

			spark.Spark.get(this.context.getConfiguration().get("api.url", DEFAULT_API_PATH)+"/ledger/identifier/:container/:key", (req, res) -> 
			{
				JSONObject responseJSON = new JSONObject();
				
				try
				{
					@SuppressWarnings("unchecked")
					Class<?> container = Serialization.getInstance().getClassForId(req.params("container").toLowerCase());
					if (container == null)
						status(responseJSON, 404, "Container "+req.params("container").toLowerCase()+" not found");
					else if (Primitive.class.isAssignableFrom(container) == false)
						status(responseJSON, 500, "Container "+req.params("container").toLowerCase()+" is not a primitive");
					else
					{
						Match matchon = Match.valueOf(req.queryParamOrDefault("match", "all").toUpperCase());
						List<Identifier> identifiers = new ArrayList<Identifier>();
						for (String secondary : req.params("key").split(","))
							identifiers.add(Identifier.from(decodeQueryValue(secondary)));
						
						if (matchon.equals(Match.ALL) && identifiers.size() > 1)
						{
							Identifier concatenated = Identifier.from(identifiers);
							identifiers.clear();
							identifiers.add(concatenated);
						}
						
						SearchResponse<Atom> atoms = null;
						Order order = Order.valueOf(req.queryParamOrDefault("order", Order.ASCENDING.toString()).toUpperCase());
						int offset = Integer.parseInt(req.queryParamOrDefault("offset", "-1"));
						int limit = Integer.parseUnsignedInt(req.queryParamOrDefault("limit", "100"));
						atoms = API.this.context.getLedger().get(new SearchQuery(identifiers, matchon, (Class<? extends Primitive>) container, order, offset, limit), Atom.class, Spin.UP);
	
						if (atoms.isEmpty() == true)
							status(responseJSON, 404, "Identifier "+req.params("container").toLowerCase()+":"+req.params("key")+" no instances found");
						else
						{
							JSONArray atomsArray  = new JSONArray();
							for (Atom atom : atoms.getResults())
								atomsArray.put(Serialization.getInstance().toJsonObject(atom, Output.ALL));
	
							responseJSON.put("atoms", atomsArray);
							status(responseJSON, 200, atoms.getQuery().getOffset(), atoms.getNextOffset(), limit, atoms.isEOR());
						}
					}
				}
				catch(Throwable t)
				{
					status(responseJSON, 500, t.toString());
					apiLog.error(t);
				}
				
				return responseJSON.toString(4);
			});

			spark.Spark.get(this.context.getConfiguration().get("api.url", DEFAULT_API_PATH)+"/ledger/atom/submit", (req, res) -> 
			{
				JSONObject responseJSON = new JSONObject();
				
				try
				{
					Hash randomValue = Hash.random();
					UniqueParticle particle = new UniqueParticle(randomValue, API.this.context.getNode().getIdentity());
					Atom atom = new Atom(particle);
					
					API.this.context.getLedger().submit(atom);
					responseJSON.put("atom", Serialization.getInstance().toJsonObject(atom, Output.ALL));
					status(responseJSON, 200);
				}
				catch(Throwable t)
				{
					status(responseJSON, 500, t.toString());
					apiLog.error(t);
				}
				
				return responseJSON.toString(4);
			});

			spark.Spark.get(this.context.getConfiguration().get("api.url", DEFAULT_API_PATH)+"/bootstrap", (req, res) -> 
			{
				JSONObject responseJSON = new JSONObject();
				
				try
				{
					int offset = Integer.parseInt(req.queryParamOrDefault("offset", "0"));
					int limit = Integer.parseUnsignedInt(req.queryParamOrDefault("limit", "100"));
					List<Peer> peers = API.this.context.getNetwork().getPeerStore().get(offset, limit, new StandardPeerFilter(API.this.context));
					
					if (peers.isEmpty())
					{
						status(responseJSON, 404, "No peers found");
					}
					else
					{
						JSONArray peersArray  = new JSONArray();
						for (Peer peer : peers)
							peersArray.put(Serialization.getInstance().toJsonObject(peer, Output.API));
						responseJSON.put("peers", peersArray);
						
						boolean EOR = peers.size() != limit;
						status(responseJSON, 200, offset, EOR == true ? -1 : offset + peers.size(), limit, EOR);
					}
				}
				catch(Throwable t)
				{
					status(responseJSON, 500, t.toString());
					apiLog.error(t);
				}
				
				return responseJSON.toString(4);
			});

			spark.Spark.post(this.context.getConfiguration().get("api.url", DEFAULT_API_PATH)+"/bootstrap", (req, res) -> 
			{
				JSONObject responseJSON = new JSONObject();

				try
				{
					String bodyString = req.body();
					JSONObject nodeJSON = new JSONObject(bodyString);
					String nodeIP = req.ip();
					URI nodeURI = Agent.getURI(nodeIP, nodeJSON.getInt("port"));
					Peer nodePeer = new Peer(nodeURI, Serialization.getInstance().fromJsonObject(nodeJSON, Node.class), Protocol.TCP, Protocol.UDP);
					API.this.context.getNetwork().getPeerStore().store(nodePeer);

					int offset = Integer.parseInt(req.queryParamOrDefault("offset", "0"));
					int limit = Integer.parseUnsignedInt(req.queryParamOrDefault("limit", "100"));
					List<Peer> peers = API.this.context.getNetwork().getPeerStore().get(offset, limit, new StandardPeerFilter(API.this.context));
					
					if (peers.isEmpty())
					{
						status(responseJSON, 404, "No peers found");
					}
					else
					{
						JSONArray peersArray  = new JSONArray();
						for (Peer peer : peers)
							peersArray.put(Serialization.getInstance().toJsonObject(peer, Output.API));
						responseJSON.put("peers", peersArray);
						
						boolean EOR = peers.size() != limit;
						status(responseJSON, 200, offset, EOR == true ? -1 : offset + peers.size(), limit, EOR);
					}
				}
				catch(Throwable t)
				{
					status(responseJSON, 500, t.toString());
					apiLog.error(t);
				}

				return responseJSON.toString(4);
			});
		}
		catch (Throwable t)
		{
			throw new StartupException(t);
		}
	}

	@Override
	public void stop() throws TerminationException
	{
		spark.Spark.stop();
	}
	
	private JSONObject status(int code)
	{
		JSONObject status = new JSONObject();
		status.put("code", code);
		return status;
	}
	
	private JSONObject status(int code, long offset, long nextOffset, int limit, boolean eor)
	{
		JSONObject status = new JSONObject();
		status.put("code", code);
		status.put("offset", offset);
		status.put("next_offset", nextOffset);
		status.put("limit", limit);
		status.put("eor", eor);
		return status;
	}

	private JSONObject status(int code, String message)
	{
		JSONObject status = new JSONObject();
		status.put("code", code);
		if (message != null && message.isEmpty() == false)
			status.put("message", message);
		return status;
	}

	private JSONObject status(int code, String message, long offset, long nextOffset, int limit, boolean eor)
	{
		JSONObject status = new JSONObject();
		status.put("code", code);
		status.put("offset", offset);
		status.put("next_offset", nextOffset);
		status.put("limit", limit);
		status.put("eor", eor);
		if (message != null && message.isEmpty() == false)
			status.put("message", message);
		return status;
	}

	private void status(JSONObject response, int code)
	{
		JSONObject status = status(code);
		response.put("status", status);
		response.put("node", Serialization.getInstance().toJsonObject(new Node(this.context.getNode()), Output.API));
	}

	private void status(JSONObject response, int code, String message)
	{
		JSONObject status = status(code, message);
		response.put("status", status);
		response.put("node", Serialization.getInstance().toJsonObject(new Node(this.context.getNode()), Output.API));
	}
	
	private void status(JSONObject response, int code, long offset, long nextOffset, int limit, boolean eor)
	{
		JSONObject status = status(code, offset, nextOffset, limit, eor);
		response.put("status", status);
		response.put("node", Serialization.getInstance().toJsonObject(new Node(this.context.getNode()), Output.API));
	}

	private void status(JSONObject response, int code, String message, long offset, long nextOffset, int limit, boolean eor)
	{
		JSONObject status = status(code, message, offset, nextOffset, limit, eor);
		response.put("status", status);
		response.put("node", Serialization.getInstance().toJsonObject(new Node(this.context.getNode()), Output.API));
	}
	
	private Class<?> decodeQueryType(String value)
	{
		if (value.startsWith(JacksonCodecConstants.BYTE_STR_VALUE) == true)
			return byte[].class;

		if (value.startsWith(JacksonCodecConstants.HASH_STR_VALUE) == true)
			return Hash.class;
		
		if (value.startsWith(JacksonCodecConstants.U256_STR_VALUE) == true)
			return UInt256.class;

		return String.class;
	}

	private Object decodeQueryValue(String value)
	{
		if (value.startsWith(JacksonCodecConstants.BYTE_STR_VALUE) == true)
			return Bytes.fromBase64String(value.substring(JacksonCodecConstants.STR_VALUE_LEN));

		if (value.startsWith(JacksonCodecConstants.HASH_STR_VALUE) == true)
			return new Hash(value.substring(JacksonCodecConstants.STR_VALUE_LEN));
		
		if (value.startsWith(JacksonCodecConstants.U256_STR_VALUE) == true)
			return UInt256.from(value.substring(JacksonCodecConstants.STR_VALUE_LEN));

		return value;
	}
}
