package org.fuserleer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.ledger.SearchResult;
import org.fuserleer.ledger.StateAddress;
import org.fuserleer.ledger.StateSearchQuery;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.HostedFileParticle;
import org.java_websocket.WebSocketImpl;

public class WebServer implements Service
{
	private class Handler extends AbstractHandler 
	{
		@Override
		public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException
		{
			String pathInfo = baseRequest.getPathInfo().toLowerCase();
			StateAddress stateAddress = new StateAddress(HostedFileParticle.class, Hash.from(pathInfo));
			Future<SearchResult> searchFuture = Context.get().getLedger().get(new StateSearchQuery(stateAddress, Atom.class));
			try
			{
				SearchResult searchResult = searchFuture.get(5, TimeUnit.SECONDS);
				if (searchResult == null)
					response.setStatus(HttpServletResponse.SC_NOT_FOUND);
				else
				{
				    Atom atom = searchResult.getPrimitive();
					if (atom == null)
						response.setStatus(HttpServletResponse.SC_NOT_FOUND);
					else
					{
						boolean found = false;
						for (HostedFileParticle hostedFileParticle : atom.getParticles(HostedFileParticle.class))
						{
							if (hostedFileParticle.getPath().compareToIgnoreCase(pathInfo) != 0)
								continue;
							
							response.setContentType(hostedFileParticle.getContentType());
							response.getOutputStream().write(hostedFileParticle.getData());
							response.getOutputStream().flush();
							found = true;
							break;
						}
	
						if (found == true)
							response.setStatus(HttpServletResponse.SC_OK);
						else
							response.setStatus(HttpServletResponse.SC_NOT_FOUND);
					}
				}
			}
			catch (TimeoutException toex)
			{
				response.setStatus(HttpServletResponse.SC_REQUEST_TIMEOUT);
			}
			catch (Exception ex)
			{
				response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
				ex.printStackTrace();
			}

		    baseRequest.setHandled(true);
		}
	}
	
	private final Context 	context;
	private final Server 	server;
	private final Handler 	handler;
	
	public WebServer(final Context context)
	{
		super();
		
		this.context = Objects.requireNonNull(context, "Context is null");
		this.server = new Server(new InetSocketAddress(context.getConfiguration().get("webserver.address", "0.0.0.0"), context.getConfiguration().get("webserver.port", WebSocketImpl.DEFAULT_PORT)));
		this.handler = new Handler();
		ContextHandler con = new ContextHandler();
		con.setContextPath("/");
		con.setHandler(this.handler);
      
		this.server.setHandler(con);
	}
	
	@Override
	public void start() throws StartupException
	{
		try
		{
			this.server.start();
		}
		catch (Exception ex)
		{
			throw new StartupException(ex);
		}
    }

	@Override
	public void stop() throws TerminationException
	{
		try
		{
			this.server.stop();
		}
		catch (Exception ex)
		{
			throw new TerminationException(ex);
		}
	}
}
