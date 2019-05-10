package org.springframework.kafka.core;

import org.junit.jupiter.api.Test;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

public class FactorySuppliedDeserializerKafkaConsumerFactoryTests {


	@Test
	public void testNoOverrides() {
		Map<String, Object> originalConfig = new HashMap<>();
		FactorySuppliedDeserializerKafkaConsumerFactory<String, String> target = new FactorySuppliedDeserializerKafkaConsumerFactory<>(originalConfig);
		assertThat(target.deriveConfigsForConsumerInstance(null, null, null, null)).isEqualTo(originalConfig);
	}

	@Test
	public void testPropertyOverrides() {
		Map<String, Object> originalConfig = Stream
				.of(new SimpleEntry<>("config1", new Object()),
						new SimpleEntry("config2", new Object()))
				.collect(Collectors.toMap(SimpleEntry<String, Object>::getKey, SimpleEntry::getValue));

		Properties overrides = new Properties();
		overrides.setProperty("config1", "overridden");
		FactorySuppliedDeserializerKafkaConsumerFactory<String, String> target = new FactorySuppliedDeserializerKafkaConsumerFactory<>(originalConfig);
		Map<String, Object> deriveConfig = target.deriveConfigsForConsumerInstance(null, null, null, overrides);
		assertThat(deriveConfig.get("config1")).isEqualTo("overridden");
		assertThat(deriveConfig.get("config2")).isSameAs(originalConfig.get("config2"));

	}

	@Test
	public void testClientIdSuffixOnDefault() {
		Map<String, Object> originalConfig = Collections.singletonMap(CLIENT_ID_CONFIG, "original");
		FactorySuppliedDeserializerKafkaConsumerFactory<String, String> target = new FactorySuppliedDeserializerKafkaConsumerFactory<>(originalConfig);
		assertThat(target.deriveConfigsForConsumerInstance(null, null, "-1", null).get(CLIENT_ID_CONFIG)).isEqualTo("original-1");

	}

	@Test
	public void testClientIdSuffixWithoutDefault() {
		FactorySuppliedDeserializerKafkaConsumerFactory<String, String> target = new FactorySuppliedDeserializerKafkaConsumerFactory<>(new HashMap<>());
		assertThat(target.deriveConfigsForConsumerInstance(null, null, "-1", null).get(CLIENT_ID_CONFIG)).isNull();

	}

	@Test
	public void testClientIdPrefixOnDefault() {
		Map<String, Object> originalConfig = Collections.singletonMap(CLIENT_ID_CONFIG, "original");
		FactorySuppliedDeserializerKafkaConsumerFactory<String, String> target = new FactorySuppliedDeserializerKafkaConsumerFactory<>(originalConfig);
		assertThat(target.deriveConfigsForConsumerInstance(null, "overridden", null, null).get(CLIENT_ID_CONFIG)).isEqualTo("overridden");

	}

	@Test
	public void testClientIdPrefixWithoutDefault() {
		FactorySuppliedDeserializerKafkaConsumerFactory<String, String> target = new FactorySuppliedDeserializerKafkaConsumerFactory<>(new HashMap<>());
		assertThat(target.deriveConfigsForConsumerInstance(null, "overridden", null, null).get(CLIENT_ID_CONFIG)).isEqualTo("overridden");

	}

	@Test
	public void testClientIdSuffixAndPrefixOnDefault() {
		Map<String, Object> originalConfig = Collections.singletonMap(CLIENT_ID_CONFIG, "original");
		FactorySuppliedDeserializerKafkaConsumerFactory<String, String> target = new FactorySuppliedDeserializerKafkaConsumerFactory<>(originalConfig);
		assertThat(target.deriveConfigsForConsumerInstance(null, "overridden", "-1", null).get(CLIENT_ID_CONFIG)).isEqualTo("overridden-1");

	}

	@Test
	public void testClientIdSuffixAndPrefixOnPropertyOverride() {
		Map<String, Object> originalConfig = Collections.singletonMap(CLIENT_ID_CONFIG, "original");
		Properties overrides = new Properties();
		overrides.setProperty(CLIENT_ID_CONFIG, "property-overridden");
		FactorySuppliedDeserializerKafkaConsumerFactory<String, String> target = new FactorySuppliedDeserializerKafkaConsumerFactory<>(originalConfig);
		assertThat(target.deriveConfigsForConsumerInstance(null, "overridden", "-1", overrides).get(CLIENT_ID_CONFIG)).isEqualTo("overridden-1");

	}


	@Test
	public void testGroupIdOnDefault() {
		Map<String, Object> originalConfig = Collections.singletonMap(GROUP_ID_CONFIG, "original");
		FactorySuppliedDeserializerKafkaConsumerFactory<String, String> target = new FactorySuppliedDeserializerKafkaConsumerFactory<>(originalConfig);
		assertThat(target.deriveConfigsForConsumerInstance("overridden", null, null, null).get(GROUP_ID_CONFIG)).isEqualTo("overridden");

	}

	@Test
	public void testGroupIdWithoutDefault() {
		FactorySuppliedDeserializerKafkaConsumerFactory<String, String> target = new FactorySuppliedDeserializerKafkaConsumerFactory<>(new HashMap<>());
		assertThat(target.deriveConfigsForConsumerInstance("overridden", null, null, null).get(GROUP_ID_CONFIG)).isEqualTo("overridden");

	}

	@Test
	public void testGroupIdOnPropertyOverride() {
		Map<String, Object> originalConfig = Collections.singletonMap(GROUP_ID_CONFIG, "original");
		Properties overrides = new Properties();
		overrides.setProperty(GROUP_ID_CONFIG, "property-overridden");
		FactorySuppliedDeserializerKafkaConsumerFactory<String, String> target = new FactorySuppliedDeserializerKafkaConsumerFactory<>(originalConfig);
		assertThat(target.deriveConfigsForConsumerInstance("overridden", null, null, overrides).get(GROUP_ID_CONFIG)).isEqualTo("overridden");

	}


}
