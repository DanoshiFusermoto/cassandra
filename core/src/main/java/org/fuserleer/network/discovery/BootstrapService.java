package org.fuserleer.network.discovery;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.fuserleer.API;
import org.fuserleer.Context;
import org.fuserleer.executors.Executable;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.Protocol;
import org.fuserleer.network.peers.Peer;
import org.fuserleer.network.peers.PeerState;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.DsonOutput.Output;

public class BootstrapService extends Executable
{
	private static final Logger networkLog = Logging.getLogger();

	private final Context context;
	private final Set<URI> hosts = new HashSet<URI>();
	
	public BootstrapService(final Context context)
	{
		this.context = Objects.requireNonNull(context);
		
		HashSet<String> unparsedHosts = new HashSet<String>();

		// allow nodes to connect to others, bypassing TLS handshake
		if(this.context.getConfiguration().get("network.discovery.allow_tls_bypass", 0) == 1) 
		{
			networkLog.info("Allowing TLS handshake bypass...");
			SSLFix.trustAllHosts();
		}

		for (String unparsedURL : this.context.getConfiguration().get("network.discovery.urls", "").split(","))
		{
			unparsedURL = unparsedURL.trim();
			if (unparsedURL.isEmpty())
				continue;

			unparsedHosts.add(unparsedURL);
		}

		for (String unparsedHost : this.context.getConfiguration().get("network.seeds", "").split(","))
			unparsedHosts.add(unparsedHost);

		for (String unparsedHost : unparsedHosts)
		{
			unparsedHost = unparsedHost.trim();
			if (unparsedHost.isEmpty())
				continue;
			try
			{
				if (unparsedHost.contains("://") == false)
					unparsedHost = API.DEFAULT_SCHEME+unparsedHost;
				
				if (this.context.getNetwork().isWhitelisted(URI.create(unparsedHost)) == false)
					continue;

				this.hosts.add(URI.create(unparsedHost.trim()));
			}
			catch (Exception ex)
			{
				networkLog.error("Could not add bootstrap "+unparsedHost.trim(), ex);
			}
		}
	}

	@Override
	public void execute()
	{
		boolean completed = false;
		List<URI> shuffledHosts = new ArrayList<URI>(this.hosts);
		Collections.shuffle(shuffledHosts);

		int connectionTimeout = this.context.getConfiguration().get("network.discovery.connection.timeout", 10000);
		int readTimeout = this.context.getConfiguration().get("network.discovery.read.timeout", 10000);
		while(completed == false && isTerminated() == false)
		{
			for (URI bootstrapHost : shuffledHosts)
			{
				networkLog.info(this.context.getName()+": Contacting boostrap host "+bootstrapHost+" for known peers ...");

				// open connection
				HttpURLConnection conn = null;
				BufferedInputStream input = null;
				try
				{
					// No custom port for bootstrap
					if (bootstrapHost.getPort() == -1)
						bootstrapHost = new URI(bootstrapHost.getScheme(), bootstrapHost.getUserInfo(), bootstrapHost.getHost(), API.DEFAULT_PORT,
												bootstrapHost.getPath(), bootstrapHost.getQuery(), bootstrapHost.getFragment());

					// No custom path for bootstrap
					if (bootstrapHost.getPath() == null || bootstrapHost.getPath().isEmpty() == true)
						bootstrapHost = new URI(bootstrapHost.getScheme(), bootstrapHost.getUserInfo(), bootstrapHost.getHost(), bootstrapHost.getPort(),
												API.DEFAULT_API_PATH+"/bootstrap", bootstrapHost.getQuery(), bootstrapHost.getFragment());
					
					conn = (HttpURLConnection) bootstrapHost.toURL().openConnection();
					// spoof User-Agents otherwise some CDNs do not let us through.
					conn.setRequestMethod("POST");
					conn.setRequestProperty("User-Agent", "curl/7.54.0");
					conn.setRequestProperty("Content-Type", "application/json; utf-8");
					conn.setRequestProperty("Accept", "application/json");
					conn.setAllowUserInteraction(false); // no follow symlinks - just plain old direct links
					conn.setUseCaches(false);
					conn.setConnectTimeout(connectionTimeout);
					conn.setReadTimeout(readTimeout);
					
					conn.setDoOutput(true);
					OutputStream os = conn.getOutputStream();
					os.write(Serialization.getInstance().toJson(this.context.getNode(), Output.WIRE).getBytes(StandardCharsets.UTF_8));
					os.flush();
					os.close();
					
					// read data
					input = new BufferedInputStream(conn.getInputStream());
					JSONTokener tokener = new JSONTokener(input);
					JSONObject response = new JSONObject(tokener);
					// TODO shouldn't be storing the bootstrap node to peers database.
					//		seems better to let that information propagate naturally
/*					JSONObject nodeJSONObject = response.getJSONObject("node");
					if (nodeJSONObject != null)
					{
						Node bootstrapNode = Serialization.getInstance().fromJsonObject(nodeJSONObject, Node.class);
						if (bootstrapNode.getIdentity().equals(this.context.getNode().getIdentity()) == true)
							continue;
						
						URI boostrapPeerURI = Agent.getURI(bootstrapHost.getHost(), bootstrapNode.getPort());
						Peer bootstrapPeer = new Peer(boostrapPeerURI, bootstrapNode, Protocol.TCP);
						this.context.getNetwork().getPeerStore().store(bootstrapPeer);
					}*/
					
					if (response.has("peers") == true)
					{
						JSONArray peersArray = response.getJSONArray("peers");
						if (peersArray != null)
						{
							for (int p = 0 ; p < peersArray.length() ; p++)
							{
								JSONObject peerJSONObject = peersArray.getJSONObject(p);
								Peer peer = Serialization.getInstance().fromJsonObject(peerJSONObject, Peer.class);
								this.context.getNetwork().getPeerStore().store(peer);
							}
						}
					}
					else
						networkLog.info(this.context.getName()+": host "+bootstrapHost+" responsed with null peers array");
				}
				catch (URISyntaxException e)
				{
					networkLog.error("unable to create bootstrap URI for host "+bootstrapHost, e);
				}
				catch (IOException e)
				{
					// rejected, offline, etc. - this is expected
					networkLog.error("host "+bootstrapHost+" is not reachable", e);
				}
				catch (RuntimeException e)
				{
					// rejected, offline, etc. - this is expected
					networkLog.warn("invalid host returned by node finder: "+bootstrapHost, e);
				}
				finally
				{
					if (input != null)
						try { input.close(); } catch (IOException ignoredExceptionOnClose) { }
				}
			}
			
			if (this.context.getNetwork().count(Protocol.TCP, PeerState.CONNECTED) > 0)
				completed = true;
			
			if (completed == false)
			{
				try
				{
					Thread.sleep(30000);
				}
				catch (InterruptedException e)
				{
					// NO NOTHING //
				}
				
				if (this.context.isStopped() == true)
					break;
			}
		}		
		
		networkLog.info(this.context.getName()+": Boostrapper terminating");
		terminate(false);
	}
}
