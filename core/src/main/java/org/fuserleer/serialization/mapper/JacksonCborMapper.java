package org.fuserleer.serialization.mapper;

import java.io.IOException;
import java.util.function.Function;

import org.fuserleer.crypto.Hash;
import org.fuserleer.utils.UInt128;
import org.fuserleer.utils.UInt256;
import org.fuserleer.utils.UInt384;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerIds;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jsonorg.JsonOrgModule;

/**
 * A Jackson {@link ObjectMapper} that will serialize and deserialize
 * to the subset of <a href="http://cbor.io/">CBOR</a> that DSON uses.
 */
public class JacksonCborMapper extends ObjectMapper {

	private static final long serialVersionUID = 4917479892309630214L;

	private JacksonCborMapper() {
		super(createCborFactory());
	}

	private static JsonFactory createCborFactory() {
		return new RadixCBORFactory();
	}

	/**
	 * Create an {@link ObjectMapper} that will serialize to/from
	 * CBOR encoded DSON.
	 *
	 * @param idLookup A {@link SerializerIds} used to perform serializer
	 * 		ID lookup
	 * @param filterProvider A {@link FilterProvider} to use for filtering
	 * 		serialized fields
	 * @return A freshly created {@link JacksonCborMapper}
	 */
	public static JacksonCborMapper create(SerializerIds idLookup, FilterProvider filterProvider) {
		SimpleModule cborModule = new SimpleModule();

		cborModule.addSerializer(SerializerDummy.class, new JacksonSerializerDummySerializer(idLookup));
		cborModule.addSerializer(String.class, new JacksonJsonStringSerializer());
		cborModule.addSerializer(Hash.class, new JacksonCborObjectBytesSerializer<>(
			Hash.class,
			JacksonCodecConstants.HASH_VALUE,
			Hash::toByteArray
		));
		cborModule.addSerializer(byte[].class, new JacksonCborObjectBytesSerializer<>(
			byte[].class,
			JacksonCodecConstants.BYTES_VALUE,
			Function.identity()
		));
		cborModule.addSerializer(UInt128.class, new JacksonCborObjectBytesSerializer<>(
			UInt128.class,
			JacksonCodecConstants.U10_VALUE,
			UInt128::toByteArray
		));
		cborModule.addSerializer(UInt256.class, new JacksonCborObjectBytesSerializer<>(
			UInt256.class,
			JacksonCodecConstants.U20_VALUE,
			UInt256::toByteArray
		));
		cborModule.addSerializer(UInt384.class, new JacksonCborObjectBytesSerializer<>(
			UInt384.class,
			JacksonCodecConstants.U30_VALUE,
			UInt384::toByteArray
		));
		
		cborModule.addDeserializer(SerializerDummy.class, new JacksonSerializerDummyDeserializer());
		cborModule.addDeserializer(String.class, new JacksonJsonStringDeserializer());
		cborModule.addDeserializer(Hash.class, new JacksonCborObjectBytesDeserializer<>(
			Hash.class,
			JacksonCodecConstants.HASH_VALUE,
			Hash::new
		));
		cborModule.addDeserializer(byte[].class, new JacksonCborObjectBytesDeserializer<>(
			byte[].class,
			JacksonCodecConstants.BYTES_VALUE,
			Function.identity()
		));
		cborModule.addDeserializer(UInt128.class, new JacksonCborObjectBytesDeserializer<>(
			UInt128.class,
			JacksonCodecConstants.U10_VALUE,
			UInt128::from
		));
		cborModule.addDeserializer(UInt256.class, new JacksonCborObjectBytesDeserializer<>(
			UInt256.class,
			JacksonCodecConstants.U20_VALUE,
			UInt256::from
		));
		cborModule.addDeserializer(UInt384.class, new JacksonCborObjectBytesDeserializer<>(
			UInt384.class,
			JacksonCodecConstants.U30_VALUE,
			UInt384::from
		));

		// Special modifier for Enum values to remove :str: leadin from front
		cborModule.setDeserializerModifier(new BeanDeserializerModifier() 
		{
			@Override
			@SuppressWarnings("rawtypes")
			public JsonDeserializer<Enum> modifyEnumDeserializer(DeserializationConfig config, final JavaType type, BeanDescription beanDesc, final JsonDeserializer<?> deserializer) 
			{
				return new JsonDeserializer<Enum>() 
				{
					@Override
					@SuppressWarnings("unchecked")
					public Enum deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException 
					{
						String name = jp.getValueAsString();
//						if (!name.startsWith(JacksonCodecConstants.STR_STR_VALUE))
//							throw new IllegalStateException(String.format("Expected value starting with %s, found: %s", JacksonCodecConstants.STR_STR_VALUE, name));

						Class<? extends Enum> rawClass = (Class<Enum<?>>) type.getRawClass();
						return Enum.valueOf(rawClass, jp.getValueAsString());//.substring(JacksonCodecConstants.STR_VALUE_LEN));
					}
				};
			}
		});

		JacksonCborMapper mapper = new JacksonCborMapper();
		mapper.registerModule(cborModule);
	    mapper.registerModule(new JsonOrgModule());
	    mapper.registerModule(new GuavaModule());

		mapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
		mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
                .withFieldVisibility(JsonAutoDetect.Visibility.NONE)
                .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withCreatorVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY));
		mapper.setSerializationInclusion(Include.NON_EMPTY);
		mapper.setFilterProvider(filterProvider);
		mapper.setAnnotationIntrospector(new DsonFilteringIntrospector());
	    mapper.setDefaultTyping(new DsonTypeResolverBuilder(idLookup));
		return mapper;
	}
}
