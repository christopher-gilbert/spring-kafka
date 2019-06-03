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
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.lang.Nullable;

/**
 * The strategy to produce {@link org.apache.kafka.common.serialization.Deserializer} instances for {@link Consumer}s
 * that are created by {@link ConsumerFactory}s.
 * <p>
 * Any implementation may leave default implementations for one (or both!) methods, but then the user of the
 * {@link ConsumerFactory} must specify {@link Deserializer} classes as appropriate in
 * spring.kafka.consumer configuration, and they must have no-argument constructors.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Chris Gilbert
 */
public interface KafkaDeserializerFactory<K, V> {

	/**
	 * Provide a {@link Deserializer} for {@link org.apache.kafka.clients.consumer.ConsumerRecord} keys.
	 *
	 * @return the Deserializer (null in the default implementation)
	 */
	default @Nullable
	Deserializer<K> getKeyDeserializer() {
		return null;
	}

	/**
	 * Provide a {@link Deserializer} for {@link org.apache.kafka.clients.consumer.ConsumerRecord} values.
	 *
	 * @return the Deserializer (null in the default implementation)
	 */
	default @Nullable
	Deserializer<V> getValueDeserializer() {
		return null;
	}

}
