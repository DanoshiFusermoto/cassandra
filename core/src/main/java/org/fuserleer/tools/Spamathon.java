package org.fuserleer.tools;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.fuserleer.Context;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.ECKeyPair;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.executors.Executable;
import org.fuserleer.executors.Executor;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.Particle;
import org.fuserleer.ledger.atoms.UniqueParticle;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.network.messaging.MessageProcessor;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerId2;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Longs;

public class Spamathon
{
	private static final Logger spammerLog = Logging.getLogger("spammer");

	private static Spamathon instance;
	
	public static Spamathon getInstance()
	{
		if (instance == null)
			instance = new Spamathon();
		
		return instance;
	}
	
	@SerializerId2("spam.initiate.message")
	public static class InitiateSpamMessage extends Message
	{
		@JsonProperty("iterations")
		@DsonOutput(Output.ALL)
		private int iterations;
		
		@JsonProperty("rate")
		@DsonOutput(Output.ALL)
		private int rate;
		
		@JsonProperty("uniques")
		@DsonOutput(Output.ALL)
		private int uniques;

		InitiateSpamMessage()
		{
			super();
		}
		
		public InitiateSpamMessage(int iterations, int rate, int uniques)
		{
			super();
			
			this.iterations = iterations;
			this.rate = rate;
			this.uniques = uniques;
		}

		public int getIterations()
		{
			return this.iterations;
		}

		public int getRate()
		{
			return this.rate;
		}

		public int getUniques()
		{
			return this.uniques;
		}
	}

	
	private Executable spammer = null;
	
	public class Spammer extends Executable
	{
		private final int iterations;
		private final int rate;
		private final int uniques;
		private final CountDownLatch latch;
		private volatile int processed = 0;

		Spammer(int iterations, int rate, int uniques)
		{
			super();
			
			this.iterations = iterations;
			this.rate = rate;
			this.uniques = uniques;
			this.latch = new CountDownLatch(iterations);
			Spamathon.this.spammer = this;
			
			// GOT IT!
			spammerLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN | Logging.WARN);
		}
		
		@Override
		public void execute()
		{
			long start = System.currentTimeMillis();

			List<ECKeyPair> owners = new ArrayList<ECKeyPair>();
			try
			{
				for (int i = 0 ; i < this.iterations / Math.log(this.iterations) ; i++)
					owners.add(new ECKeyPair());
			}
			catch (CryptoException ex)
			{
				// Should never happen //
				throw new RuntimeException(ex);
			}
			
			try
			{
				int i = 0;
				int batch = Math.max(1, this.rate);
				List<Context> contexts = new ArrayList<Context>(Context.getAll());
				while(i < this.iterations)
				{
					long batchStart = System.currentTimeMillis();
					for (int b = 0 ; b < batch & i < this.iterations ; b++)
					{
						try
						{
							List<Particle> particles = new ArrayList<Particle>();
							for (int u = 0 ; u < this.uniques ; u++)
							{
								long value = ThreadLocalRandom.current().nextLong();
								Hash valueHash = new Hash(Longs.toByteArray(value), Mode.STANDARD);
								particles.add(new UniqueParticle(valueHash, owners.get(ThreadLocalRandom.current().nextInt(owners.size())).getPublicKey()));
							}
							
							Atom atom = new Atom(particles);
							try
							{
								if (spammerLog.hasLevel(Logging.DEBUG))
									spammerLog.debug("Submitting "+(i+1)+"/"+this.iterations+" of spam atoms to ledger "+atom.getHash());
								else if (i > 0 && i % 1000 == 0)
									spammerLog.info("Submitted "+i+"/"+this.iterations+" of spam atoms to ledger");
	
								// TODO selectable context
								Collections.shuffle(contexts);
								contexts.get(0).getLedger().submit(atom);
								this.processed++;
								i++;
							}
							catch (Throwable t)
							{
								spammerLog.error(t);
							}
						}
						finally
						{
							this.latch.countDown();
						}
					}
					
					try
					{
						long batchDuration = System.currentTimeMillis() - batchStart;
						if (batchDuration < 1000)
							Thread.sleep(TimeUnit.SECONDS.toMillis(1) - batchDuration);
					}
					catch (InterruptedException e)
					{
						// Do nothing //
					}
				}
			}
			finally
			{
				spammerLog.info("Spammer took "+(System.currentTimeMillis() - start)+"ms to perform "+this.iterations+" events");
			}
			
			Spamathon.this.spammer = null;
		}

		public int getNumIterations()
		{
			return this.iterations;
		}
		
		public int getNumProcessed()
		{
			return this.processed;
		}
		
		public boolean completed(long timeout, TimeUnit unit)
		{
			try
			{
				return this.latch.await(timeout, unit);
			}
			catch (InterruptedException e)
			{
				// DO NOTHING //
			}
			
			return false;
		}
	}
	
	private Spamathon()
	{
		Context.get().getNetwork().getMessaging().register(InitiateSpamMessage.class, this.getClass(), new MessageProcessor<InitiateSpamMessage>() 
		{
			@Override
			public void process(InitiateSpamMessage message, ConnectedPeer peer)
			{
				if (Spamathon.this.spammer != null)
					throw new IllegalStateException("Already an instance of spammer running");
				
				spam(message.getIterations(), message.getRate(), message.getUniques());
			}
		});
	}
	
	public boolean isSpamming()
	{
		return this.spammer == null ? false : true;
	}
	
	public Spammer spam(int iterations, int rate, int uniques)
	{
		if (Spamathon.getInstance().isSpamming() == true)
			throw new IllegalStateException("Already an instance of spammer running");
		
		Spammer spammer = new Spammer(iterations, rate, uniques);
		Executor.getInstance().submit(spammer);
		return spammer;
	}
}
