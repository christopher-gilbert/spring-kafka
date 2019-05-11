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

import org.apache.kafka.common.serialization.Serializer;

/**
 * The strategy to produce {@link Serializer} instances for {@link org.apache.kafka.clients.producer.Producer}s
 * that are created by {@link ProducerFactory}s.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Chris Gilbert
 */
public interface KafkaSerializerFactory<K, V> {

	/**
	 * Provide a {@link Serializer} for {@link org.apache.kafka.clients.producer.ProducerRecord} keys.
	 *
	 * @return the Serializer (null in the default implementation)
	 */
	default Serializer<K> getKeySerializer() {
		return null;
	}

	/**
	 * Provide a {@link Serializer} for {@link org.apache.kafka.clients.producer.ProducerRecord} keys, designed for a
	 * specific {@link ProducerFactory}.
	 *
	 * @return the Serializer (null in the default implementation)
	 */
	default Serializer<K> getKeySerializer(String producerFactoryBeanName) {
		return null;
	}

	/**
	 * Provide a {@link Serializer} for {@link org.apache.kafka.clients.producer.ProducerRecord} values.
	 *
	 * @return the Serializer (null in the default implementation)
	 */
	default Serializer<V> getValueSerializer() {
		return null;
	}

	/**
	 * Provide a {@link Serializer} for {@link org.apache.kafka.clients.producer.ProducerRecord} values, designed for
	 * a specific {@link ProducerFactory}.
	 *
	 * @return the Serializer (null in the default implementation)
	 */
	default Serializer<V> getValueSerializer(String producerFactoryBeanName) {
		return null;
	}

}
