package org.fuserleer;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.fuserleer.crypto.ECKeyPair;
import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.Universe;
import org.fuserleer.ledger.Block;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.time.Time;
import org.fuserleer.time.WallClockTime;
import org.fuserleer.utils.Bytes;

import java.io.File;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class GenerateUniverses
{
	private static final Logger LOGGER = Logging.getLogger("generate_universes");

	public static final String RADIX_ICON_URL = "https://assets.radixdlt.com/icons/icon-xrd-32x32.png";

	private final ECKeyPair universeKey;
	private final ECKeyPair nodeKey;
	private final Configuration configuration;

	public GenerateUniverses(String[] arguments) throws Exception 
	{
		this.configuration = new Configuration("commandline_options.json", arguments);

		Security.addProvider(new BouncyCastleProvider());

		Time.createAsDefault(new WallClockTime(this.configuration));
		
		String deploymentKeyPath = this.configuration.get("universe.key.path", "universe.key");
		this.universeKey = ECKeyPair.fromFile(new File(deploymentKeyPath), true);

		// TODO want to be able to specify multiple nodes to get the genesis mass as bootstrapping
		String nodeKeyPath = this.configuration.get("node.key.path", "node.key");
		this.nodeKey = ECKeyPair.fromFile(new File(nodeKeyPath), true);
	}

	public GenerateUniverses() throws Exception 
	{
		this(new String[] { "universe.key" });
	}

	public List<Universe> generateDeployments() throws Exception 
	{
		LOGGER.info("UNIVERSE KEY PRIVATE:  "+Bytes.toHexString(this.universeKey.getPrivateKey()));
		LOGGER.info("UNIVERSE KEY PUBLIC:   "+Bytes.toHexString(this.universeKey.getPublicKey().getBytes()));
		LOGGER.info("NODE KEY PRIVATE:  "+Bytes.toHexString(this.nodeKey.getPrivateKey()));
		LOGGER.info("NODE KEY PUBLIC:   "+Bytes.toHexString(this.nodeKey.getPublicKey().getBytes()));

		List<Universe> universes = new ArrayList<>();

		long universeTimestampSeconds = this.configuration.get("universe.timestamp", 1136073600);
		long universeTimestampMillis = TimeUnit.SECONDS.toMillis(universeTimestampSeconds);

		universes.add(buildUniverse(10000, "Mainnet", "The public universe", Universe.Type.PRODUCTION, universeTimestampMillis, (int) TimeUnit.DAYS.toSeconds(1)));
		universes.add(buildUniverse(20000, "Testnet", "The test universe", Universe.Type.TEST, universeTimestampMillis, (int) TimeUnit.HOURS.toSeconds(1)));
		universes.add(buildUniverse(30000, "Devnet", "The development universe", Universe.Type.DEVELOPMENT, universeTimestampMillis, (int) TimeUnit.HOURS.toSeconds(1)));

		return universes;
	}

	private Universe buildUniverse(int port, String name, String description, Universe.Type type, long timestamp, int epoch) throws Exception 
	{
		byte universeMagic = (byte) (Universe.computeMagic(this.universeKey.getPublicKey(), timestamp, epoch, port, type) & 0xFF);
		Block universeBlock = createGenesisBlock(universeMagic, timestamp);

		Universe universe = Universe.newBuilder()
			.port(port)
			.name(name)
			.description(description)
			.type(type)
			.timestamp(timestamp)
			.epoch(epoch)
			.creator(this.universeKey.getPublicKey())
			.setGenesis(universeBlock)
			.build();
		universe.sign(this.universeKey);

		if (universe.verify(this.universeKey.getPublicKey()) == false)
			throw new ValidationException("Signature failed for " + name + " deployment");
		
		System.out.println(Serialization.getInstance().toJsonObject(universe, Output.WIRE).toString(4));
		byte[] deploymentBytes = Serialization.getInstance().toDson(universe, Output.WIRE);
		System.out.println("UNIVERSE - " + type + ": "+Bytes.toBase64String(deploymentBytes));

		return universe;
	}

	private Block createGenesisBlock(byte magic, long timestamp) throws Exception 
	{
		Block genesisBlock = new Block(0l, 0l, Hash.ZERO, Hash.ZERO, Collections.singleton(new Atom()));
		return genesisBlock;
	}

	public static void main(String[] arguments) throws Exception 
	{
		GenerateUniverses generateDeployments = new GenerateUniverses(arguments);
		generateDeployments.generateDeployments();
	}
}
