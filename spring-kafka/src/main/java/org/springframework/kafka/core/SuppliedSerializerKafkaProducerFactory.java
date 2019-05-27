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
import org.springframework.lang.Nullable;

import java.util.Map;
import java.util.function.Supplier;

/**
 * {@link ProducerFactory} that makes use of a {@link KafkaSerializerFactory} which uses supplier functions to get
 * key and value {@link Serializer}s for each {@link org.apache.kafka.clients.producer.Producer} it creates, if provided.
 * If none is provided but {@link Serializer} classes are specified in application properties then new instances of
 * those classes are created for each producer. That approach is simpler if you require separate instances per
 * {@link org.apache.kafka.clients.producer.Producer} AND your {@link Serializer}s have no-arg constructors. You can
 * use configuration for either of the {@link Serializer}s by simply providing a Supplier that returns null.
 *
 * If you cannot use configuration to specify {@link Serializer}s and do not absolutely require separate instances
 * for each {@link org.apache.kafka.clients.producer.Producer} you can use the {@link DefaultKafkaProducerFactory},
 * which uses shared {@link Serializer} instances for each {@link org.apache.kafka.clients.producer.Producer}.
 *
 * In most cases, this is safe, as {@link org.apache.kafka.clients.producer.Producer}s are pooled and generally not
 * closed, but if you have more than one {@link ProducerFactory} in your application context, using the same {@link Serializer}s
 * there is a risk that the {@link Serializer}s may get closed by one {@link ProducerFactory} rendering them
 * unusable by the other.
 *
 * @param <K> the key type in produced {@link org.apache.kafka.clients.producer.ProducerRecord}s
 * @param <V> the value type in consumed {@link org.apache.kafka.clients.producer.ProducerRecord}s
 * @author Chris Gilbert
 */
public class SuppliedSerializerKafkaProducerFactory<K, V> extends KafkaProducerFactory<K, V> {

	/**
	 * Construct a factory with the provided configuration.
	 *
	 * @param configs the configuration.
	 */
	public SuppliedSerializerKafkaProducerFactory(Map<String, Object> configs) {
		this(configs, () -> null, () -> null);
	}

	/**
	 * Construct a factory with the provided configuration and {@link Serializer}s.
	 * *
	 *
	 * @param configs         the configuration.
	 * @param keySerializerSupplier   the key {@link Serializer} supplier function.
	 * @param valueSerializerSupplier the value {@link Serializer} supplier function.
	 */
	public SuppliedSerializerKafkaProducerFactory(Map<String, Object> configs,
												  @Nullable Supplier<Serializer<K>> keySerializerSupplier,
												  @Nullable Supplier<Serializer<V>> valueSerializerSupplier) {
		super(configs, new SupplierKafkaSerializerFactory<>(keySerializerSupplier == null ? () -> null : keySerializerSupplier,
				valueSerializerSupplier == null ? () -> null : valueSerializerSupplier));
	}

	/**
	 * Simple implementation of {@link KafkaSerializerFactory} that provides a {@link Serializer} instance from
	 * its supplier, if not null (for null Suppliers, it is essential to configure a deserializer class).
	 */
	private static class SupplierKafkaSerializerFactory<K, V> implements KafkaSerializerFactory<K, V> {

		private final Supplier<Serializer<K>> keySerializerSupplier;

		private final Supplier<Serializer<V>> valueSerializerSupplier;

		SupplierKafkaSerializerFactory(Supplier<Serializer<K>> keySerializerSupplier,
									   Supplier<Serializer<V>> valueSerializerSupplier) {
			this.keySerializerSupplier = keySerializerSupplier;
			this.valueSerializerSupplier = valueSerializerSupplier;
		}

		@Override
		public @Nullable
		Serializer<K> getKeySerializer() {
			return this.keySerializerSupplier.get();
		}

		@Override
		public @Nullable
		Serializer<V> getValueSerializer() {
			return this.valueSerializerSupplier.get();
		}

	}

}
