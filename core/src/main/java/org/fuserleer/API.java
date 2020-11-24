package org.fuserleer;

import java.util.List;
import java.util.Objects;

import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.ledger.BlockHeader;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.UniqueParticle;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
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
			
			spark.Spark.get(this.context.getConfiguration().get("api.url", DEFAULT_API_PATH)+"/ledger/atom/:hash", (req, res) -> 
			{
				JSONObject responseJSON = new JSONObject();
				
				try
				{
					Atom atom = API.this.context.getLedger().get(new Hash(req.params("hash")), Atom.class);
					if (atom == null)
						status(responseJSON, 404, "Atom "+req.params("hash")+" not found");
					else
					{
						responseJSON.put("atom", Serialization.getInstance().toJsonObject(atom, Output.ALL));
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
