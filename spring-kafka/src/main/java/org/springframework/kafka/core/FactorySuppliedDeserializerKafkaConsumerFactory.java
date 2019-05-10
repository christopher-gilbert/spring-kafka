/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.kafka.core;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

/**
 * KafkaConsumerFactory that makes use of a {@link KafkaDeserializerFactory} to construct key and value
 * deserializers for each Consumer that is constructed.
 *
 * Users may provide their own implementation of {@link KafkaDeserializerFactory}, or alternatively an
 * {@link BeanLookupKafkaDeserializerFactory} is implicitly created and populated by any
 * Deserializers annotated as {@link org.springframework.kafka.annotation.KafkaKeyDeserializer} or
 * {@link org.springframework.kafka.annotation.KafkaValueDeserializer}
 *
 * @param <K> the key type in consumed {@link org.apache.kafka.clients.consumer.ConsumerRecord}s
 * @param <V> the value type in consumed {@link org.apache.kafka.clients.consumer.ConsumerRecord}s
 * @author Chris Gilbert (based on original {@link DefaultKafkaConsumerFactory}
 */
public class FactorySuppliedDeserializerKafkaConsumerFactory<K, V> implements ConsumerFactory<K, V>, BeanNameAware {

	private final Map<String, Object> configs;

	private KafkaDeserializerFactory<K, V> deserializerFactory;

	private String name;

	/**
	 * Construct a factory with the provided configuration.
	 *
	 * @param configs the configuration.
	 */
	public FactorySuppliedDeserializerKafkaConsumerFactory(Map<String, Object> configs) {
		this(configs, null);
	}

	/**
	 * Construct a factory with the provided configuration and factory for deserializers.
	 *
	 * @param configs             the configuration.
	 * @param deserializerFactory the factory for providing key and value deserializer instances
	 */
	public FactorySuppliedDeserializerKafkaConsumerFactory(Map<String, Object> configs,
														   KafkaDeserializerFactory<K, V> deserializerFactory) {
		this.configs = new HashMap<>(configs);
		this.deserializerFactory = deserializerFactory;
	}

	public void setDeserializerFactory(KafkaDeserializerFactory<K, V> deserializerFactory) {
		this.deserializerFactory = deserializerFactory;
	}

	public boolean hasDeserializerFactory() {
		return this.deserializerFactory != null;
	}

	public KafkaDeserializerFactory<K, V> getDeserializerFactory() {
		return this.deserializerFactory;
	}

	@Override
	public Map<String, Object> getConfigurationProperties() {
		return Collections.unmodifiableMap(this.configs);
	}

	@Override
	public Deserializer<K> getKeyDeserializer() {
		return this.deserializerFactory.getKeyDeserializer(this.name);
	}

	@Override
	public Deserializer<V> getValueDeserializer() {
		return this.deserializerFactory.getValueDeserializer(this.name);
	}

	@Override
	public Consumer<K, V> createConsumer(@Nullable String groupId, @Nullable String clientIdPrefix,
										 @Nullable String clientIdSuffix) {

		return createConsumer(groupId, clientIdPrefix, clientIdSuffix, null);
	}

	@Override
	public Consumer<K, V> createConsumer(@Nullable String groupId, @Nullable String clientIdPrefix,
										 @Nullable final String clientIdSuffix, @Nullable Properties properties) {

		return new KafkaConsumer<>(deriveConfigsForConsumerInstance(groupId, clientIdPrefix, clientIdSuffix, properties), this.getKeyDeserializer(), this.getValueDeserializer());
	}

	@Override
	public boolean isAutoCommit() {
		Object auto = this.configs.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
		return auto instanceof Boolean ? (Boolean) auto
				: auto instanceof String ? Boolean.valueOf((String) auto) : true;
	}

	@Override
	public void setBeanName(String name) {
		this.name = name;
	}


	protected Map<String, Object> deriveConfigsForConsumerInstance(@Nullable String groupId, @Nullable String clientIdPrefix,
																   @Nullable final String clientIdSuffix, @Nullable Properties properties) {
		String clientId = deriveClientId(clientIdPrefix, clientIdSuffix);
		if (propertiesAreOverridden(properties) || clientIdIsOverridden(clientId) || groupIdIsOverridden(groupId)) {
			return deriveConfigs(clientId, groupId, properties);
		}
		return this.configs;
	}

	/**
	 * client id suffix is appended to the client id prefix which overrides the
	 * {@code client.id} property, if present in config.
	 *
	 * @param clientIdPrefix
	 * @param clientIdSuffix
	 * @return
	 */
	private String deriveClientId(@Nullable String clientIdPrefix,
								  @Nullable final String clientIdSuffix) {
		String clientId = this.configs.get(CLIENT_ID_CONFIG) != null ? this.configs.get(CLIENT_ID_CONFIG).toString() : null;
		clientId = StringUtils.hasText(clientIdPrefix) ? clientIdPrefix : clientId;
		if (clientId != null) {
			clientId += StringUtils.hasText(clientIdSuffix) ? clientIdSuffix : "";
		}
		return clientId;
	}

	private boolean propertiesAreOverridden(@Nullable Properties properties) {
		return (properties != null && !properties.isEmpty());
	}

	private boolean clientIdIsOverridden(@Nullable String clientId) {
		return clientId != null && !clientId.equals(this.configs.get(CLIENT_ID_CONFIG));
	}

	private boolean groupIdIsOverridden(@Nullable String groupId) {
		return groupId != null && !groupId.equals(this.configs.get(GROUP_ID_CONFIG));
	}


	private Map<String, Object> deriveConfigs(@Nullable String clientId, @Nullable String groupId, @Nullable Properties properties) {
		Map<String, Object> modifiedConfigs = new HashMap<>(this.configs);

		if (propertiesAreOverridden(properties)) {
			modifiedConfigs.putAll(properties.stringPropertyNames()
											 .stream()
											 .collect(Collectors.toMap(Function.identity(), name -> properties.getProperty(name))));
		}
		if (groupIdIsOverridden(groupId)) {
			modifiedConfigs.put(GROUP_ID_CONFIG, groupId);
		}


		if (clientIdIsOverridden(clientId)) {
			modifiedConfigs.put(CLIENT_ID_CONFIG, clientId);
		}
		return modifiedConfigs;
	}
}
