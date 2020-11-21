package org.fuserleer.events;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.fuserleer.Context;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.SubscriberExceptionContext;
import com.google.common.eventbus.SubscriberExceptionHandler;

public final class Events
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
	private final EventBus		eventBus; 
	private final AsyncEventBus	asyncEventBus; 
	private final ExecutorService				eventExecutorService;
	private final ExecutorThreadFactory			eventExecutorThreadFactory;
	private final SubscriberExceptionHandler	exceptionHandler; 
	private final SubscriberExceptionHandler	asyncExceptionHandler; 

	public Events(Context context)
	{
		this.context = Objects.requireNonNull(context);
		this.eventExecutorThreadFactory = new ExecutorThreadFactory();
		
		// TODO have EventHandler threads configurable?
		int eventThreads = Math.max(1, Runtime.getRuntime().availableProcessors()/2);
		if (this.context.getConfiguration().getCommandLine().hasOption("contexts") == true)
			eventThreads = Math.max(1, Runtime.getRuntime().availableProcessors() / Integer.parseInt(this.context.getConfiguration().getCommandLine().getOptionValue("contexts")));
		this.eventExecutorService = Executors.newFixedThreadPool(eventThreads, this.eventExecutorThreadFactory);
		
		this.exceptionHandler = new SubscriberExceptionHandler() 
		{
			@Override
			public void handleException(Throwable arg0, SubscriberExceptionContext arg1) 
			{
				// TODO Auto-generated method stub
			}
		};
		this.eventBus = new EventBus(this.exceptionHandler);
		
		this.asyncExceptionHandler = new SubscriberExceptionHandler() 
		{
			@Override
			public void handleException(Throwable arg0, SubscriberExceptionContext arg1) 
			{
				// TODO Auto-generated method stub
			}
		};
		this.asyncEventBus = new AsyncEventBus(this.eventExecutorService, this.asyncExceptionHandler); 
	}

	public void post(final Event event)
	{
		this.eventBus.post(event);
		
		if ((event instanceof SynchronousEvent) == false)
			this.asyncEventBus.post(event);
	}

	public void register(final EventListener listener)
	{
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
		if (listener instanceof SynchronousEventListener)
			this.eventBus.unregister(listener);
		else
			this.asyncEventBus.unregister(listener);
	}
}
