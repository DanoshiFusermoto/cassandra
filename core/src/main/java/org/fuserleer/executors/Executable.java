package org.fuserleer.executors;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

public abstract class Executable implements Runnable
{
	private final long id = System.nanoTime();
	private final AtomicReference<Future<?>> future = new AtomicReference<>();

	private boolean terminated = false;
	private boolean cancelled = false;
	private boolean finished = false;
	private CountDownLatch finishLatch;

	public final void terminate(boolean finish)
	{
		this.terminated = true;
		Future<?> thisFuture = this.future.get();
		if (thisFuture != null && thisFuture.cancel(false) == false && finish)
			finish();
	}

	public final boolean isTerminated()
	{
		return this.terminated;
	}

	public final boolean isCancelled()
	{
		return this.cancelled;
	}

	public final boolean isFinished()
	{
		return this.finished;
	}

	public abstract void execute();

	public void cancelled()
	{
		
	}

	@Override
	public final synchronized void run()
	{
		try
		{
			if (this.cancelled == true)
				throw new IllegalStateException("Executable "+this+" is cancelled");

			if (this.terminated == true)
				throw new IllegalStateException("Executable "+this+" is terminated");

			this.finished = false;
			this.finishLatch = new CountDownLatch(1);
			execute();
		}
		// TODO check this isnt needed
		catch (Throwable t)
		{
			// FIXME weird exception happens here on startup
			Future<?> thisFuture = this.future.get();
			if (thisFuture != null) {
				thisFuture.cancel(false);
			}

			this.terminated = true;
		}
		finally
		{
			this.finished = true;
			this.finishLatch.countDown();
		}
	}
	
	public final synchronized boolean cancel()
	{
		try
		{
			if (this.cancelled == true)
				throw new IllegalStateException("Executable "+this+" is cancelled");

			if (this.terminated == true)
				throw new IllegalStateException("Executable "+this+" is terminated");
			
			Future<?> thisFuture = this.future.get();
			if (thisFuture != null)
			{
				if (thisFuture.cancel(false) == true)
				{
					cancelled();
					return true;
				}
			}
			
			return false;
		}
		finally
		{
			this.cancelled = true;
		}
	}
	public final long getID()
	{
		return this.id;
	}

	public final Future<?> getFuture()
	{
		return this.future.get();
	}

	final void setFuture(final Future<?> future)
	{
		Objects.requireNonNull(future, "Executable future is null");
		this.future.set(future);
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == null)
			return false;

		if (obj == this)
			return true;

		if (!(obj instanceof Executable))
			return false;

		return this.id == ((Executable)obj).id;
	}

	@Override
	public int hashCode()
	{
		return (int) (this.id & 0xFFFFFFFF);
	}

	@Override
	public String toString()
	{
		return "ID: "+this.id+" Terminated: "+this.terminated;
	}

	void finish()
	{
		try 
		{
			if (this.finishLatch != null)
				this.finishLatch.await();
		} 
		catch (InterruptedException e) 
		{
			// Re-raise
			Thread.currentThread().interrupt();
		}
	}
}
