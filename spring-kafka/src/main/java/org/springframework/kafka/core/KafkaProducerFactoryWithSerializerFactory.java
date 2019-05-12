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
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
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
 * {@link ProducerFactory} that makes use of a {@link KafkaSerializerFactory} to construct key and value
 * serializers for each Producer that is constructed.
 *
 * Users may provide their own implementation of {@link KafkaSerializerFactory}, or alternatively an
 * {@link BeanLookupKafkaSerializerFactory} is implicitly created and populated with any
 * serializers annotated as {@link org.springframework.kafka.annotation.KafkaKeySerializer} or
 * {@link org.springframework.kafka.annotation.KafkaValueSerializer}
 *
 * @param <K> the key type in produced {@link org.apache.kafka.clients.producer.ProducerRecord}s
 * @param <V> the value type in consumed {@link org.apache.kafka.clients.producer.ProducerRecord}s
 * @author Chris Gilbert (based on original {@link DefaultKafkaProducerFactory}
 */
public class KafkaProducerFactoryWithSerializerFactory<K, V> implements ProducerFactory<K, V>, BeanNameAware {

	private final Map<String, Object> configs;

	private KafkaSerializerFactory<K, V> serializerFactory;

	private String name;

	/**
	 * Construct a factory with the provided configuration.
	 *
	 * @param configs the configuration.
	 */
	public KafkaProducerFactoryWithSerializerFactory(Map<String, Object> configs) {
		this(configs, null);
	}

	/**
	 * Construct a factory with the provided configuration and factory for serializers.
	 *
	 * @param configs             the configuration.
	 * @param serializerFactory the factory for providing key and value serializer instances
	 */
	public KafkaProducerFactoryWithSerializerFactory(Map<String, Object> configs,
													 @Nullable  KafkaSerializerFactory<K, V> serializerFactory) {
		this.configs = new HashMap<>(configs);
		this.serializerFactory = serializerFactory;
	}

	public void setSerializerFactory(KafkaSerializerFactory<K, V> serializerFactory) {
		this.serializerFactory = serializerFactory;
	}

	public boolean hasSerializerFactory() {
		return this.serializerFactory != null;
	}

	public KafkaSerializerFactory<K, V> getSerializerFactory() {
		return this.serializerFactory;
	}

	@Override
	public void setBeanName(String name) {
		this.name = name;
	}

	@Override
	public Producer<K, V> createProducer() {
		return null;
	}

	@Override
	public boolean transactionCapable() {
		return false;
	}

	@Override
	public void closeProducerFor(String transactionIdSuffix) {

	}

	@Override
	public boolean isProducerPerConsumerPartition() {
		return false;
	}
}
