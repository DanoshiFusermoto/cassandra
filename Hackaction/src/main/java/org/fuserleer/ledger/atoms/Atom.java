package org.fuserleer.ledger.atoms;

import org.fuserleer.BasicObject;
import org.fuserleer.ledger.Primitive;
import org.fuserleer.ledger.StatePrimitive;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.atom")
public final class Atom extends BasicObject implements Primitive, StatePrimitive
{
	public Atom()
	{
		super();
	}
}
