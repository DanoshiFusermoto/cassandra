package org.fuserleer.crypto;

import org.apache.milagro.amcl.BLS381.BIG;
import org.apache.milagro.amcl.BLS381.ECP2;
import org.apache.milagro.amcl.BLS381.ROM;
import org.fuserleer.crypto.bls.group.G2Point;

/**
 * Forked from https://github.com/ConsenSys/mikuli/tree/master/src/main/java/net/consensys/mikuli/crypto
 * 
 * Modified for use with Cassandra as internal code not a dependency
 * 
 * Original repo source has no license headers.
 */
public class BLSCurveParameters 
{
  static public final G2Point g2Generator() { return new G2Point(ECP2.generator()); }
  static public final BIG curveOrder() { return new BIG(ROM.CURVE_Order); }
}
