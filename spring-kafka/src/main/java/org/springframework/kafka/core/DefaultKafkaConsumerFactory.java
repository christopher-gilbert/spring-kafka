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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.lang.Nullable;

import java.util.Map;
import java.util.Properties;

/**
 * The {@link ConsumerFactory} implementation to produce new {@link Consumer} instances
 * for provided {@link Map} {@code configs} and optional {@link Deserializer} {@code keyDeserializer},
 * {@code valueDeserializer} implementations on each {@link #createConsumer()}
 * invocation.
 * <p>
 * Note that the same {@link Deserializer} instances are shared by all the created {@link KafkaConsumer}s. If you are
 * using Deserializers that cannot be reused once closed then you should use a
 * {@link KafkaConsumerFactory} with a {@link KafkaDeserializerFactory} that provides new instances
 * on retrieval or use {@link SuppliedDeserializerKafkaConsumerFactory}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Gary Russell
 * @author Murali Reddy
 * @author Artem Bilan
 * @author Chris Gilbert (moved original implementation to {@link KafkaConsumerFactory})
 */
public class DefaultKafkaConsumerFactory<K, V> implements ConsumerFactory<K, V> {

	private final KafkaConsumerFactory<K, V> delegate;

	private final SingleInstanceKafkaDeserializerFactory deserializerFactory;

	/**
	 * Construct a factory with the provided configuration.
	 *
	 * @param configs the configuration.
	 */
	public DefaultKafkaConsumerFactory(Map<String, Object> configs) {
		this(configs, null, null);
	}

	/**
	 * Construct a factory with the provided configuration and deserializers.
	 *
	 * @param configs           the configuration.
	 * @param keyDeserializer   the key {@link Deserializer}.
	 * @param valueDeserializer the value {@link Deserializer}.
	 */
	public DefaultKafkaConsumerFactory(Map<String, Object> configs,
									   @Nullable Deserializer<K> keyDeserializer,
									   @Nullable Deserializer<V> valueDeserializer) {
		this.deserializerFactory = new SingleInstanceKafkaDeserializerFactory(keyDeserializer, valueDeserializer);
		this.delegate = new KafkaConsumerFactory<>(configs, this.deserializerFactory);
	}

	// retained for backward compatibility
	public void setKeyDeserializer(@Nullable Deserializer<K> keyDeserializer) {
		this.deserializerFactory.setKeyDeserializer(keyDeserializer);
	}

	// retained for backward compatibility
	public void setValueDeserializer(@Nullable Deserializer<V> valueDeserializer) {
		this.deserializerFactory.setValueDeserializer(valueDeserializer);
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


	@Deprecated
	protected KafkaConsumer<K, V> createKafkaConsumer(@Nullable String groupId,
													  @Nullable String clientIdPrefix,
													  @Nullable final String clientIdSuffixArg) {

		return createKafkaConsumer(groupId, clientIdPrefix, clientIdSuffixArg, null);
	}

	protected KafkaConsumer<K, V> createKafkaConsumer(@Nullable String groupId,
													  @Nullable String clientIdPrefix,
													  @Nullable final String clientIdSuffixArg,
													  @Nullable Properties properties) {

		return (KafkaConsumer<K, V>) this.delegate.createConsumer(groupId, clientIdPrefix, clientIdSuffixArg, properties);
	}


	// retained for backward compatibility
	protected KafkaConsumer<K, V> createKafkaConsumer(Map<String, Object> configs) {
		return (KafkaConsumer<K, V>) new DefaultKafkaConsumerFactory<K, V>(configs).createConsumer();
	}

	/**
	 * Simple implementation of {@link KafkaDeserializerFactory} that provides the same {@link Deserializer} instances
	 * every time UNLESS the {@link Deserializer} instance variables are modified between calls. These fields are only
	 * mutable in order to honour the public mutators in {@link DefaultKafkaConsumerFactory}, and are not expected
	 * to be modified after initial creation.
	 */
	private class SingleInstanceKafkaDeserializerFactory implements KafkaDeserializerFactory<K, V> {

		private Deserializer<K> keyDeserializer;

		private Deserializer<V> valueDeserializer;

		SingleInstanceKafkaDeserializerFactory(@Nullable Deserializer<K> keyDeserializer, @Nullable Deserializer<V> valueDeserializer) {
			this.keyDeserializer = keyDeserializer;
			this.valueDeserializer = valueDeserializer;
		}

		@Override
		public Deserializer<K> getKeyDeserializer() {
			return this.keyDeserializer;
		}

		@Override
		public Deserializer<V> getValueDeserializer() {
			return this.valueDeserializer;
		}

		void setKeyDeserializer(@Nullable Deserializer<K> keyDeserializer) {
			this.keyDeserializer = keyDeserializer;
		}

		void setValueDeserializer(@Nullable Deserializer<V> valueDeserializer) {
			this.valueDeserializer = valueDeserializer;
		}

	}

}
