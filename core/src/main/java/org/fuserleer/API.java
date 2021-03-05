package org.fuserleer;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.fuserleer.common.Agent;
import org.fuserleer.common.Match;
import org.fuserleer.common.Order;
import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.ledger.AssociationSearchQuery;
import org.fuserleer.ledger.AssociationSearchResponse;
import org.fuserleer.ledger.Block;
import org.fuserleer.ledger.BlockHeader;
import org.fuserleer.ledger.BlockHeader.InventoryType;
import org.fuserleer.ledger.SearchResult;
import org.fuserleer.ledger.StateAddress;
import org.fuserleer.ledger.StateSearchQuery;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.ledger.atoms.Particle;
import org.fuserleer.ledger.atoms.Particle.Spin;
import org.fuserleer.ledger.atoms.UniqueParticle;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.Protocol;
import org.fuserleer.network.discovery.StandardDiscoveryFilter;
import org.fuserleer.network.peers.Peer;
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
					processed.put("local", API.this.context.getMetaData().get("ledger.processed.atoms.local", 0l));
					processed.put("total", API.this.context.getMetaData().get("ledger.processed.atoms.total", 0l));
					statistics.put("processed", processed);

					JSONObject throughput = new JSONObject();
					throughput.put("local", API.this.context.getMetaData().get("ledger.throughput.atoms.local", 0l));
					throughput.put("total", API.this.context.getMetaData().get("ledger.throughput.atoms.total", 0l));
					throughput.put("shards", API.this.context.getMetaData().get("ledger.throughput.shards.touched", 0l));
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
					long offset = block.getHeader().getIndex()+block.getHeader().getInventory(InventoryType.ATOMS).size();
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
					long offset = block.getHeader().getIndex()+block.getHeader().getInventory(InventoryType.ATOMS).size();
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
					SearchResult searchResult = null;
					StateAddress stateAddress = new StateAddress(Atom.class, Hash.from(decodeQueryValue(req.params("hash"))));
					Future<SearchResult> atomFuture = API.this.context.getLedger().get(new StateSearchQuery(stateAddress, Atom.class));
					searchResult = atomFuture.get(5, TimeUnit.SECONDS);
					if (searchResult == null)
						status(responseJSON, 404, "Atom containing particle "+req.params("hash")+" not found");
					else
					{
						Atom atom = searchResult.getPrimitive();
						responseJSON.put("atom", Serialization.getInstance().toJsonObject(atom, Output.ALL));
						
						Future<SearchResult> certificateFuture = API.this.context.getLedger().get(new StateSearchQuery(new StateAddress(Atom.class, atom.getHash()), AtomCertificate.class));
						searchResult = certificateFuture.get(5, TimeUnit.SECONDS);
						if (searchResult != null) 
						{
							try
							{
								AtomCertificate certificate = searchResult.getPrimitive();
								responseJSON.put("certificate", Serialization.getInstance().toJsonObject(certificate, Output.ALL));
							}
							catch (Exception ex)
							{
								apiLog.warn(API.this.context.getName()+": Certificate for atom "+atom.getHash()+" failed", ex);
							}
						}
						
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
					SearchResult searchResult = null;
					StateAddress stateAddress = new StateAddress(Particle.class, Hash.from(decodeQueryValue(req.params("hash"))));
					Future<SearchResult> atomFuture = API.this.context.getLedger().get(new StateSearchQuery(stateAddress, Atom.class));

					searchResult = atomFuture.get(5, TimeUnit.SECONDS);
					if (searchResult == null)
						status(responseJSON, 404, "Atom containing particle "+req.params("hash")+" not found");
					else
					{
						Atom atom = searchResult.getPrimitive();
						responseJSON.put("atom", Serialization.getInstance().toJsonObject(atom, Output.ALL));
						
						Future<SearchResult> certificateFuture = API.this.context.getLedger().get(new StateSearchQuery(new StateAddress(Atom.class, atom.getHash()), AtomCertificate.class));
						searchResult = certificateFuture.get(5, TimeUnit.SECONDS);
						if (searchResult != null) 
						{
							try
							{
								AtomCertificate certificate = searchResult.getPrimitive();
								responseJSON.put("certificate", Serialization.getInstance().toJsonObject(certificate, Output.ALL));
							}
							catch (Exception ex)
							{
								apiLog.warn(API.this.context.getName()+": Certificate for atom "+atom.getHash()+" containing particle "+req.params("hash")+" failed", ex);
							}
						}
						
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

			spark.Spark.get(this.context.getConfiguration().get("api.url", DEFAULT_API_PATH)+"/ledger/state/:scope/:key", (req, res) -> 
			{
				JSONObject responseJSON = new JSONObject();
				
				try
				{
					StateAddress stateAddress = new StateAddress(Hash.from(decodeQueryValue(req.params("scope"))), Hash.from(decodeQueryValue(req.params("key"))));
					Future<SearchResult> atomFuture = API.this.context.getLedger().get(new StateSearchQuery(stateAddress, Atom.class));

					SearchResult searchResult = atomFuture.get(5, TimeUnit.SECONDS);
					if (searchResult == null) 
						status(responseJSON, 404, "Atom containing state "+req.params("scope")+":"+req.params("key")+" not found");
					else
					{
						Atom atom = searchResult.getPrimitive();
						responseJSON.put("atom", Serialization.getInstance().toJsonObject(atom, Output.ALL));
						
						Future<SearchResult> certificateFuture = API.this.context.getLedger().get(new StateSearchQuery(new StateAddress(Atom.class, atom.getHash()), AtomCertificate.class));
						searchResult = certificateFuture.get(5, TimeUnit.SECONDS);
						if (searchResult != null) 
						{
							try
							{
								AtomCertificate certificate = searchResult.getPrimitive();
								responseJSON.put("certificate", Serialization.getInstance().toJsonObject(certificate, Output.ALL));
							}
							catch (Exception ex)
							{
								apiLog.warn(API.this.context.getName()+": Certificate for atom containing state "+req.params("scope")+":"+req.params("key").toLowerCase()+" failed", ex);
							}
						}
						
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

			spark.Spark.get(this.context.getConfiguration().get("api.url", DEFAULT_API_PATH)+"/ledger/association", (req, res) -> 
			{
				JSONObject responseJSON = new JSONObject();
				
				try
				{
					Match matchon = Match.valueOf(req.queryParamOrDefault("match", "all").toUpperCase());
					List<Hash> associations = new ArrayList<Hash>();
					for (String secondary : req.queryParamOrDefault("keys", "").split(","))
						associations.add(Hash.from(decodeQueryValue(secondary)));
					
					if (matchon.equals(Match.ALL) && associations.size() > 1)
					{
						Hash concatenated = Hash.from(associations);
						associations.clear();
						associations.add(concatenated);
					}
					
					Order order = Order.valueOf(req.queryParamOrDefault("order", Order.ASCENDING.toString()).toUpperCase());
					int offset = Integer.parseInt(req.queryParamOrDefault("offset", "-1"));
					int limit = Integer.parseUnsignedInt(req.queryParamOrDefault("limit", "100"));
					Future<AssociationSearchResponse> responseFuture = API.this.context.getLedger().get(new AssociationSearchQuery(associations, matchon, Atom.class, order, offset, limit), Spin.UP);
					Collection<SearchResult> responseResults = responseFuture.get(5, TimeUnit.SECONDS).getResults();

					if (responseResults.isEmpty() == true)
						status(responseJSON, 404, "No instances found associated with "+req.queryParamOrDefault("keys", ""));
					else
					{
						JSONArray atomsArray  = new JSONArray();
						for (SearchResult result : responseResults)
						{
							Atom atom  = result.getPrimitive();
							atomsArray.put(Serialization.getInstance().toJsonObject(atom, Output.ALL));
						}

						responseJSON.put("atoms", atomsArray);
						status(responseJSON, 200, responseFuture.get().getQuery().getOffset(), responseFuture.get().getNextOffset(), limit, responseFuture.get().isEOR());
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
					List<Peer> peers = API.this.context.getNetwork().getPeerStore().get(offset, limit, new StandardDiscoveryFilter(API.this.context));
					
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
					URI nodeURI = Agent.getURI(nodeIP, nodeJSON.getInt("network_port"));
					Peer nodePeer = new Peer(nodeURI, Serialization.getInstance().fromJsonObject(nodeJSON, Node.class), Protocol.TCP, Protocol.UDP);
					API.this.context.getNetwork().getPeerStore().store(nodePeer);

					int offset = Integer.parseInt(req.queryParamOrDefault("offset", "0"));
					int limit = Integer.parseUnsignedInt(req.queryParamOrDefault("limit", "100"));
					List<Peer> peers = API.this.context.getNetwork().getPeerStore().get(offset, limit, new StandardDiscoveryFilter(API.this.context));
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

		return value.toLowerCase();
	}
}
