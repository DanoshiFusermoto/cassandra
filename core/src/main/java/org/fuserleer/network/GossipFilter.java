package org.fuserleer.network;

import java.util.Objects;
import java.util.Set;

import org.fuserleer.Context;
import org.fuserleer.common.Primitive;

public abstract class GossipFilter
{
	private final Context context;
	
	public GossipFilter(Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
	}
	
	Context getContext()
	{
		return this.context;
	}
	
	public abstract Set<Long> filter(Primitive object) throws Throwable;
}
