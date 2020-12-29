package org.fuserleer;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.fuserleer.crypto.ECKeyPair;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.Universe;
import org.fuserleer.ledger.Block;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.Particle.Spin;
import org.fuserleer.ledger.atoms.TokenSpecification;
import org.fuserleer.ledger.atoms.TransferParticle;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.time.Time;
import org.fuserleer.time.WallClockTime;
import org.fuserleer.utils.Bytes;
import org.fuserleer.utils.UInt128;
import org.fuserleer.utils.UInt256;

import java.io.File;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

public final class GenerateUniverses
{
	private static final Logger LOGGER = Logging.getLogger("generate_universes");

	public static final String RADIX_ICON_URL = "https://assets.radixdlt.com/icons/icon-xrd-32x32.png";

	private final ECKeyPair universeKey;
	private final Set<ECPublicKey> nodeKeys;
	private final Configuration configuration;

	public GenerateUniverses(String[] arguments) throws Exception 
	{
		this.configuration = new Configuration("commandline_options.json", arguments);

		Security.addProvider(new BouncyCastleProvider());

		Time.createAsDefault(new WallClockTime(this.configuration));
		
		String deploymentKeyPath = this.configuration.get("universe.key.path", "universe.key");
		this.universeKey = ECKeyPair.fromFile(new File(deploymentKeyPath), true);

		// TODO want to be able to specify multiple nodes to get the genesis mass as bootstrapping
//		String nodeKeys = this.configuration.get("node.keys", "A4LUF3ravj4MwMtlYGc3+kiRDB7NcsB141xCgd8DhhBf,AtOM21m9f9DxaR7i2zpM1HNfzazSziwJv9smNsg9JHsO,A8h8Em/ml6X5I5amEMg/Mdz0PgcBwAI3gTUTTPCcjDyU");
		String nodeKeys = this.configuration.get("node.keys", "AihcVMYB7ndhmWCsfj0ll8U/CsUy9Kh/7Zb3J7g3dYv5");
		this.nodeKeys = new LinkedHashSet<ECPublicKey>();
		
		StringTokenizer nodeKeysTokenizer = new StringTokenizer(nodeKeys, ",");
		while (nodeKeysTokenizer.hasMoreTokens() == true)
		{
			String nodeKeyToken = nodeKeysTokenizer.nextToken();
			this.nodeKeys.add(ECPublicKey.from(nodeKeyToken));
		}
	}

	public GenerateUniverses() throws Exception 
	{
		this(new String[] { "universe.key" });
	}

	public List<Universe> generateDeployments() throws Exception 
	{
		LOGGER.info("UNIVERSE KEY PRIVATE:  "+Bytes.toHexString(this.universeKey.getPrivateKey()));
		LOGGER.info("UNIVERSE KEY PUBLIC:   "+Bytes.toHexString(this.universeKey.getPublicKey().getBytes()));

		List<Universe> universes = new ArrayList<>();

		long universeTimestampSeconds = this.configuration.get("universe.timestamp", 1136073600);
		long universeTimestampMillis = TimeUnit.SECONDS.toMillis(universeTimestampSeconds);

		universes.add(buildUniverse(10000, "Mainnet", "The public universe", Universe.Type.PRODUCTION, universeTimestampMillis, 2, (int) TimeUnit.DAYS.toSeconds(1)));
		universes.add(buildUniverse(20000, "Testnet", "The test universe", Universe.Type.TEST, universeTimestampMillis, 2, (int) TimeUnit.HOURS.toSeconds(1)));
		universes.add(buildUniverse(30000, "Devnet", "The development universe", Universe.Type.DEVELOPMENT, universeTimestampMillis, 2, (int) TimeUnit.HOURS.toSeconds(1)));

		return universes;
	}

	private Universe buildUniverse(int port, String name, String description, Universe.Type type, long timestamp, int shardGroups, int epoch) throws Exception 
	{
		byte universeMagic = (byte) (Universe.computeMagic(this.universeKey.getPublicKey(), timestamp, shardGroups, epoch, port, type) & 0xFF);
		Block universeBlock = createGenesisBlock(universeMagic, timestamp);

		Universe universe = Universe.newBuilder()
			.port(port)
			.name(name)
			.description(description)
			.type(type)
			.timestamp(timestamp)
			.epoch(epoch)
			.shardGroups(shardGroups)
			.creator(this.universeKey.getPublicKey())
			.setGenesis(universeBlock)
			.setGenodes(this.nodeKeys)
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
		final TokenSpecification tokenParticle = new TokenSpecification("FLEX", "Flexathon token", this.universeKey.getPublicKey());
		tokenParticle.sign(this.universeKey);
		final TransferParticle transferParticle = new TransferParticle(UInt256.from(UInt128.HIGH_BIT), tokenParticle.getHash(), Spin.UP, this.universeKey.getPublicKey());
		transferParticle.sign(this.universeKey);

		final List<Atom> atoms = Collections.singletonList(new Atom(tokenParticle, transferParticle));
		Block genesisBlock = new Block(0l, Hash.ZERO, UInt256.ZERO, 0, timestamp, this.universeKey.getPublicKey(), atoms, Collections.emptyList());
		genesisBlock.getHeader().sign(this.universeKey);
		return genesisBlock;
	}

	public static void main(String[] arguments) throws Exception 
	{
		GenerateUniverses generateDeployments = new GenerateUniverses(arguments);
		generateDeployments.generateDeployments();
	}
}
