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

/**
 * KafkaConsumerFactory that makes use of a {@link KafkaDeserializerFactory} to construct key and value
 * deserializers for each Consumer that is constructed.
 *
 * Users may provide their own implementation of {@link KafkaDeserializerFactory}, or alternatively an
 * {@link AnnotationDrivenKafkaDeserializerFactory} is implicitly created and populated by any
 * Deserializers annotated as {@link org.springframework.kafka.annotation.KafkaKeyDeserializer} or
 * {@link org.springframework.kafka.annotation.KafkaValueDeserializer}
 *
 * @param <K> the key type in consumed {@link org.apache.kafka.clients.consumer.ConsumerRecord}s
 * @param <V> the value type in consumed {@link org.apache.kafka.clients.consumer.ConsumerRecord}s
 * @author Chris Gilbert
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

		return createKafkaConsumer(groupId, clientIdPrefix, clientIdSuffix, null);
	}

	@Override
	public Consumer<K, V> createConsumer(@Nullable String groupId, @Nullable String clientIdPrefix,
										 @Nullable final String clientIdSuffixArg, @Nullable Properties properties) {

		return createKafkaConsumer(groupId, clientIdPrefix, clientIdSuffixArg, properties);
	}

	@Deprecated
	protected KafkaConsumer<K, V> createKafkaConsumer(@Nullable String groupId, @Nullable String clientIdPrefix,
													  @Nullable final String clientIdSuffixArg) {

		return createKafkaConsumer(groupId, clientIdPrefix, clientIdSuffixArg, null);
	}

	protected KafkaConsumer<K, V> createKafkaConsumer(@Nullable String groupId, @Nullable String clientIdPrefix,
													  @Nullable final String clientIdSuffixArg, @Nullable Properties properties) {

		boolean overrideClientIdPrefix = StringUtils.hasText(clientIdPrefix);
		String clientIdSuffix = clientIdSuffixArg;
		if (clientIdSuffix == null) {
			clientIdSuffix = "";
		}
		boolean shouldModifyClientId = (this.configs.containsKey(ConsumerConfig.CLIENT_ID_CONFIG)
				&& StringUtils.hasText(clientIdSuffix)) || overrideClientIdPrefix;
		if (groupId == null
				&& (properties == null || properties.stringPropertyNames().size() == 0)
				&& !shouldModifyClientId) {
			return createKafkaConsumer(this.configs);
		} else {
			return createConsumerWithAdjustedProperties(groupId, clientIdPrefix, properties, overrideClientIdPrefix,
					clientIdSuffix, shouldModifyClientId);
		}
	}

	private KafkaConsumer<K, V> createConsumerWithAdjustedProperties(String groupId, String clientIdPrefix,
																	 Properties properties, boolean overrideClientIdPrefix, String clientIdSuffix,
																	 boolean shouldModifyClientId) {

		Map<String, Object> modifiedConfigs = new HashMap<>(this.configs);
		if (groupId != null) {
			modifiedConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		}
		if (shouldModifyClientId) {
			modifiedConfigs.put(ConsumerConfig.CLIENT_ID_CONFIG,
					(overrideClientIdPrefix ? clientIdPrefix
							: modifiedConfigs.get(ConsumerConfig.CLIENT_ID_CONFIG)) + clientIdSuffix);
		}
		if (properties != null) {
			properties.stringPropertyNames()
					  .stream()
					  .filter(name -> !name.equals(ConsumerConfig.CLIENT_ID_CONFIG)
							  && !name.equals(ConsumerConfig.GROUP_ID_CONFIG))
					  .forEach(name -> modifiedConfigs.put(name, properties.getProperty(name)));
		}
		return createKafkaConsumer(modifiedConfigs);
	}

	protected KafkaConsumer<K, V> createKafkaConsumer(Map<String, Object> configs) {
		return new KafkaConsumer<>(configs, this.getKeyDeserializer(), this.getValueDeserializer());
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
}
