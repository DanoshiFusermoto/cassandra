package org.fuserleer.ledger.atoms;

import org.fuserleer.BasicObject;
import org.fuserleer.common.Primitive;
import org.fuserleer.common.StatePrimitive;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.atom")
public final class Atom extends BasicObject implements Primitive, StatePrimitive
{
	public Atom()
	{
		super();
	}
}
