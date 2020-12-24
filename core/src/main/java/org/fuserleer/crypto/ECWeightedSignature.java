package org.fuserleer.crypto;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Objects;

import org.bouncycastle.asn1.ASN1InputStream;
import org.bouncycastle.asn1.ASN1Integer;
import org.bouncycastle.asn1.DLSequence;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerConstants;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.utils.Bytes;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("crypto.weighted.ecdsa_signature")
public final class ECWeightedSignature
{
	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;

	@JsonProperty("version")
	@DsonOutput(Output.ALL)
	private short version = 100;

	@JsonProperty("weight")
	@DsonOutput(Output.ALL)
	private int weight;

	/* The two components of the signature. */
	private BigInteger r;
	private BigInteger s;

	public ECWeightedSignature()
	{
		this(BigInteger.ZERO, BigInteger.ZERO, 0);
	}

	/**
     * Constructs a signature with the given components and a weight. Does NOT automatically canonicalise the signature.
     */
    public ECWeightedSignature(BigInteger r, BigInteger s, int weight)
    {
    	super();

    	if (weight < 0)
    		throw new IllegalArgumentException("Weight is negative");
    	
    	this.weight = weight;
    	this.r = Objects.requireNonNull(r);
        this.s = Objects.requireNonNull(s);
    }

	public int getWeight() 
	{
		return this.weight;
	}

	public BigInteger getR() {
		return r;
	}

	public BigInteger getS() {
		return s;
	}

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        	return true;

        if (o instanceof ECWeightedSignature) 
        {
        	ECWeightedSignature signature = (ECWeightedSignature) o;
        	return Objects.equals(this.r, signature.r) && Objects.equals(this.s, signature.s) && Objects.equals(this.weight, signature.weight);
        }
        
        return false;
    }

	@Override
	public int hashCode() 
	{
		return Objects.hash(this.r, this.s, this.weight);
	}
	
	@JsonProperty("r")
	@DsonOutput(Output.ALL)
	private byte[] getJsonR() 
	{
		return Bytes.trimLeadingZeros(r.toByteArray());
	}

	@JsonProperty("s")
	@DsonOutput(Output.ALL)
	private byte[] getJsonS() 
	{
		return Bytes.trimLeadingZeros(s.toByteArray());
	}

	@JsonProperty("r")
	private void setJsonR(byte[] r) 
	{
		// Set sign to positive to stop BigInteger interpreting high bit as sign
		this.r = new BigInteger(1, r);
	}

	@JsonProperty("s")
	private void setJsonS(byte[] s) 
	{
		// Set sign to positive to stop BigInteger interpreting high bit as sign
		this.s = new BigInteger(1, s);
	}
	
	public static ECWeightedSignature decodeFromDER(byte[] bytes) 
	{
		try (ASN1InputStream decoder = new ASN1InputStream(bytes)) 
		{
			DLSequence seq = (DLSequence) decoder.readObject();
			ASN1Integer r = (ASN1Integer) seq.getObjectAt(0);
			ASN1Integer s = (ASN1Integer) seq.getObjectAt(1);
			ASN1Integer w = (ASN1Integer) seq.getObjectAt(2);
			return new ECWeightedSignature(r.getPositiveValue(), s.getPositiveValue(), w.intPositiveValueExact());
		} 
		catch (IOException e) 
		{
			throw new IllegalArgumentException("Failed to read bytes as ASN1 decode bytes", e);
		} 
		catch (ClassCastException e) 
		{
			throw new IllegalStateException("Failed to cast to ASN1Integer", e);
		}
	}
}
