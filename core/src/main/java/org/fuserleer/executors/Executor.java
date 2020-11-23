package org.fuserleer.executors;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;

public class Executor
{
	private static final Logger log = Logging.getLogger("executor");
	private static final int MAX_IMMEDIATE_EXECUTION_THREADS = 8;
	private static final int MAX_SCHEDULED_EXECUTION_THREADS = 4;

	private static Executor	instance = null;

	public static Executor getInstance()
	{
		if (instance == null)
			instance = new Executor();

		return instance;
	}

	private final 	ExecutorService 			immediateExecutor;
	private final 	ScheduledExecutorService 	scheduledExecutor;

	private Executor()
	{
		// Shared between contexts so can use all available processors up to max
		int maxImmediateExecutors = Math.min(MAX_IMMEDIATE_EXECUTION_THREADS, Runtime.getRuntime().availableProcessors());
		int maxScheduledExecutors = Math.min(MAX_SCHEDULED_EXECUTION_THREADS, Runtime.getRuntime().availableProcessors()/2);
		this.immediateExecutor = Executors.newFixedThreadPool(maxImmediateExecutors);
		this.scheduledExecutor = Executors.newScheduledThreadPool(maxScheduledExecutors);
	}

	public Executor(int numImmediateThreads, int numScheduledThreads)
	{
		this.immediateExecutor = Executors.newFixedThreadPool(numImmediateThreads);
		this.scheduledExecutor = Executors.newScheduledThreadPool(numScheduledThreads);
	}

	public Executor(int numImmediateThreads, ThreadFactory immediateThreadFactory, int numScheduledThreads, ThreadFactory scheduledThreadFactory)
	{
		this.immediateExecutor = Executors.newFixedThreadPool(numImmediateThreads, immediateThreadFactory);
		this.scheduledExecutor = Executors.newScheduledThreadPool(numScheduledThreads, scheduledThreadFactory);
	}

	public Future<?> schedule(final ScheduledExecutable executable)
	{
		executable.setFuture(this.scheduledExecutor.schedule(executable, executable.getInitialDelay(), executable.getTimeUnit()));
		return executable.getFuture();
	}

	public Future<?> scheduleWithFixedDelay(final ScheduledExecutable executable)
	{
		executable.setFuture(this.scheduledExecutor.scheduleWithFixedDelay(executable, executable.getInitialDelay(), executable.getRecurrentDelay(), executable.getTimeUnit()));
		return executable.getFuture();
	}

	public Future<?> scheduleAtFixedRate(final ScheduledExecutable executable)
	{
		executable.setFuture(this.scheduledExecutor.scheduleAtFixedRate(executable, executable.getInitialDelay(), executable.getRecurrentDelay(), executable.getTimeUnit()));
		return executable.getFuture();
	}

	public Future<?> schedule(final Executable executable, int initialDelay, TimeUnit unit)
	{
		executable.setFuture(this.scheduledExecutor.schedule(executable, initialDelay, unit));
		return executable.getFuture();
	}

	public Future<?> schedule(final Runnable runnable, int initialDelay, TimeUnit unit)
	{
		return this.scheduledExecutor.schedule(runnable, initialDelay, unit);
	}

	public Future<?> submit(final Callable<?> callable)
	{
		return this.immediateExecutor.submit(callable);
	}

	public Future<?> submit(final Executable executable)
	{
		Future<?> future = this.immediateExecutor.submit(executable);
		executable.setFuture(future);
		return future;
	}
}
