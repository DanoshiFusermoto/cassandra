package org.fuserleer.events;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.SubscriberExceptionContext;
import com.google.common.eventbus.SubscriberExceptionHandler;

public final class Events implements Service
{
	private static final Logger eventLog = Logging.getLogger("events");

	private class ExecutorThreadFactory implements ThreadFactory
	{
		private AtomicInteger nextID = new AtomicInteger(0);
		private final Set<Thread> threads = new HashSet<Thread>();

		@Override
		public Thread newThread(Runnable runnable)
		{
			Thread thread = new Thread(runnable, Events.this.context.getName()+" Event Thread "+(this.nextID.getAndIncrement()));
			thread.setDaemon(true);
			this.threads.add(thread);
			return thread;
		}
	}

	private final Context		context;
	private final SubscriberExceptionHandler	exceptionHandler; 
	private final SubscriberExceptionHandler	asyncExceptionHandler; 

	private EventBus			eventBus; 
	private AsyncEventBus		asyncEventBus; 
	private ExecutorService		eventExecutorService;
	private ExecutorThreadFactory	eventExecutorThreadFactory;

	public Events(final Context context)
	{
		this.context = Objects.requireNonNull(context);
		
		this.exceptionHandler = new SubscriberExceptionHandler() 
		{
			@Override
			public void handleException(Throwable throwable, SubscriberExceptionContext arg1) 
			{
				eventLog.error(Events.this.context.getName(), throwable);
			}
		};
		
		this.asyncExceptionHandler = new SubscriberExceptionHandler() 
		{
			@Override
			public void handleException(Throwable throwable, SubscriberExceptionContext arg1) 
			{
				eventLog.error(Events.this.context.getName(), throwable);
			}
		};
	}
	
	@Override
	public void start() throws StartupException
	{
		this.eventExecutorThreadFactory = new ExecutorThreadFactory();
		
		// TODO have EventHandler threads configurable?
		int eventThreads = Math.max(1, Runtime.getRuntime().availableProcessors()/2);
		if (this.context.getConfiguration().getCommandLine().hasOption("contexts") == true)
			eventThreads = Math.max(1, Runtime.getRuntime().availableProcessors() / Integer.parseInt(this.context.getConfiguration().getCommandLine().getOptionValue("contexts")));
		this.eventExecutorService = Executors.newFixedThreadPool(eventThreads, this.eventExecutorThreadFactory);
		
		this.eventBus = new EventBus(this.exceptionHandler);
		this.asyncEventBus = new AsyncEventBus(this.eventExecutorService, this.asyncExceptionHandler); 
	}

	@Override
	public void stop() throws TerminationException
	{
		this.asyncEventBus = null;
		this.eventBus = null;
		this.eventExecutorService.shutdown();
	}

	public void post(final Event event)
	{
		Objects.requireNonNull(event, "Event is null");
		
		this.eventBus.post(event);
		
		if ((event instanceof SynchronousEvent) == false)
			this.asyncEventBus.post(event);
	}

	public void register(final EventListener listener)
	{
		Objects.requireNonNull(listener, "Event listener to register is null");

		// TODO need some restrictions regarding the registration of sync events
		// otherwise there are means to deadlock the internal core via intricate
		// event signalling (or refactor the relevant core components ... )
		if (listener instanceof SynchronousEventListener)
			this.eventBus.register(listener);
		else
			this.asyncEventBus.register(listener);
	}

	public void unregister(final EventListener listener)
	{
		Objects.requireNonNull(listener, "Event listener to unregister is null");

		if (listener instanceof SynchronousEventListener)
			this.eventBus.unregister(listener);
		else
			this.asyncEventBus.unregister(listener);
	}
}
