/*
 * Copyright 2016-2019 the original author or authors.
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

import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * A {@link ConsumerFactory} implementation to produce new {@link Consumer} instances
 * using provided {@link Map} {@code configs} and optional {@link Deserializer} instances obtained from
 * {@code keyDeserializerSupplier} and {@code valueDeserializerSupplier} functions on each {@link #createConsumer()}
 * invocation.
 * <p>
 * Note that if either of the Suppliers is null (or returns null when called), then it is essential to configure a
 * Deserializer class in spring.kafka.consumer configuration.
 * <p>
 * Use of a Supplier allows each consumer to have separate instances of {@link Deserializer}s, which means that the
 * {@link Consumer}s can be safely closed (close cascades down to the Deserializers). If your Deserializers can be
 * created with a no argument constructor, then simpler to use configuration instead, which also results in separate
 * instances per {@link Consumer}.
 * <p>
 * If you are happy for all consumers to share the same Deserializer instances (ie if they have a no-op close
 * implementation), then you can use {@link DefaultKafkaConsumerFactory}. If you have more complex requirements,
 * then simply implement {@link KafkaDeserializerFactory} and pass it to a {@link KafkaConsumerFactory}.
 *
 * @param <K> the key type in consumed {@link org.apache.kafka.clients.consumer.ConsumerRecord}s
 * @param <V> the value type in consumed {@link org.apache.kafka.clients.consumer.ConsumerRecord}s
 * @author Chris Gilbert
 */
public class SuppliedDeserializerKafkaConsumerFactory<K, V> implements ConsumerFactory<K, V> {

	private final KafkaConsumerFactory<K, V> delegate;

	public SuppliedDeserializerKafkaConsumerFactory(Map<String, Object> configs) {
		this(configs, null, null);
	}

	/**
	 * Construct a factory with the provided configuration and deserializer suppliers.
	 *
	 * @param configs                   the configuration.
	 * @param keyDeserializerSupplier   the key {@link Deserializer} supplier function.
	 * @param valueDeserializerSupplier the value {@link Deserializer} supplier function.
	 */
	public SuppliedDeserializerKafkaConsumerFactory(Map<String, Object> configs,
													@Nullable Supplier<Deserializer<K>> keyDeserializerSupplier,
													@Nullable Supplier<Deserializer<V>> valueDeserializerSupplier) {
		this.delegate = new KafkaConsumerFactory<>(configs,
				new SupplierKafkaDeserializerFactory(keyDeserializerSupplier == null ? () -> null : keyDeserializerSupplier,
						valueDeserializerSupplier == null ? () -> null : valueDeserializerSupplier));
	}

	@Override
	public Map<String, Object> getConfigurationProperties() {
		return this.delegate.getConfigurationProperties();
	}

	@Override
	public Deserializer<K> getKeyDeserializer() {
		return this.delegate.getKeyDeserializer();
	}

	@Override
	public Deserializer<V> getValueDeserializer() {
		return this.delegate.getValueDeserializer();
	}

	@Override
	public Consumer<K, V> createConsumer(@Nullable String groupId,
										 @Nullable String clientIdPrefix,
										 @Nullable String clientIdSuffix) {

		return this.delegate.createConsumer(groupId, clientIdPrefix, clientIdSuffix);
	}

	@Override
	public Consumer<K, V> createConsumer(@Nullable String groupId,
										 @Nullable String clientIdPrefix,
										 @Nullable final String clientIdSuffixArg,
										 @Nullable Properties properties) {

		return this.delegate.createConsumer(groupId, clientIdPrefix, clientIdSuffixArg, properties);
	}

	@Override
	public boolean isAutoCommit() {
		return this.delegate.isAutoCommit();
	}


	/**
	 * Simple implementation of {@link KafkaDeserializerFactory} that provides a {@link Deserializer} instance from
	 * its supplier.
	 */
	private class SupplierKafkaDeserializerFactory implements KafkaDeserializerFactory<K, V> {

		private final Supplier<Deserializer<K>> keyDeserializerSupplier;

		private final Supplier<Deserializer<V>> valueDeserializerSupplier;

		SupplierKafkaDeserializerFactory(Supplier<Deserializer<K>> keyDeserializerSupplier,
										 Supplier<Deserializer<V>> valueDeserializerSupplier) {
			this.keyDeserializerSupplier = keyDeserializerSupplier;
			this.valueDeserializerSupplier = valueDeserializerSupplier;
		}

		@Override
		public @Nullable
		Deserializer<K> getKeyDeserializer() {
			return this.keyDeserializerSupplier.get();
		}

		@Override
		public @Nullable
		Deserializer<V> getValueDeserializer() {
			return this.valueDeserializerSupplier.get();
		}

	}

}
