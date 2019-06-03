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

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextStoppedEvent;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.support.TransactionSupport;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * The {@link ProducerFactory} implementation for a {@code singleton} shared {@link Producer} instance.
 * <p>
 * This implementation will return the same {@link Producer} instance (if transactions are
 * not enabled) for the provided {@link java.util.Map} {@code configs} and optional {@link KafkaSerializerFactory}
 * implementations on each {@link #createProducer()} invocation.
 * <p>
 * The {@link Producer} is wrapped and the underlying {@link KafkaProducer} instance is
 * not actually closed when {@link Producer#close()} is invoked. The {@link KafkaProducer}
 * is physically closed when {@link DisposableBean#destroy()} is invoked or when the
 * application context publishes a {@link ContextStoppedEvent}. You can also invoke
 * {@link #reset()}.
 * <p>
 * Setting {@link #setTransactionIdPrefix(String)} enables transactions; in which case, a
 * cache of producers is maintained; closing a producer returns it to the cache. The
 * producers are closed and the cache is cleared when the factory is destroyed, the
 * application context stopped, or the {@link #reset()} method is called.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Gary Russell
 * @author Murali Reddy
 * @author Nakul Mishra
 * @author Artem Bilan
 * @author Chris Gilbert
 */
public class KafkaProducerFactory<K, V> implements ProducerFactory<K, V>, ApplicationContextAware,
		ApplicationListener<ContextStoppedEvent>, DisposableBean {

	/**
	 * The default close timeout duration as 30 seconds.
	 */
	public static final Duration DEFAULT_PHYSICAL_CLOSE_TIMEOUT = Duration.ofSeconds(30);

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(KafkaProducerFactory.class));

	private final Map<String, Object> configs;

	private final AtomicInteger transactionIdSuffix = new AtomicInteger();

	private final BlockingQueue<CloseSafeProducer<K, V>> cache = new LinkedBlockingQueue<>();

	private final Map<String, CloseSafeProducer<K, V>> consumerProducers = new HashMap<>();

	private KafkaSerializerFactory<K, V> serializerFactory;

	private Duration physicalCloseTimeout = DEFAULT_PHYSICAL_CLOSE_TIMEOUT;

	private String transactionIdPrefix;

	private ApplicationContext applicationContext;

	private boolean producerPerConsumerPartition = true;

	private volatile CloseSafeProducer<K, V> producer;

	/**
	 * Construct a factory with the provided configuration.
	 *
	 * @param configs the configuration.
	 */
	public KafkaProducerFactory(Map<String, Object> configs) {
		this(configs, new KafkaSerializerFactory<K, V>() {
			@Override
			public Serializer<K> getKeySerializer() {
				return null;
			}

			@Override
			public Serializer<V> getValueSerializer() {
				return null;
			}
		});
	}

	/**
	 * Construct a factory with the provided configuration and {@link KafkaSerializerFactory}.
	 * Also configures a {@link #transactionIdPrefix} as a value from the
	 * {@link ProducerConfig#TRANSACTIONAL_ID_CONFIG} if provided.
	 * This config is going to be overridden with a suffix for target {@link Producer} instance.
	 *
	 * @param configs           the configuration.
	 * @param serializerFactory the key and value {@link KafkaSerializerFactory}.
	 */
	public KafkaProducerFactory(Map<String, Object> configs,
								KafkaSerializerFactory<K, V> serializerFactory) {

		this.configs = new HashMap<>(configs);
		this.serializerFactory = serializerFactory;

		String txId = (String) this.configs.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
		if (StringUtils.hasText(txId)) {
			setTransactionIdPrefix(txId);
			LOGGER.info(() -> "If 'setTransactionIdPrefix()' is not going to be configured, "
					+ "the existing 'transactional.id' config with value: '" + txId
					+ "' will be suffixed for concurrent transactions support.");
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	public KafkaSerializerFactory<K, V> getSerializerFactory() {
		return this.serializerFactory;
	}

	/**
	 * The time to wait when physically closing the producer (when {@link #reset()} or {@link #destroy()} is invoked).
	 * Specified in seconds; default {@link #DEFAULT_PHYSICAL_CLOSE_TIMEOUT}.
	 *
	 * @param physicalCloseTimeout the timeout in seconds.
	 * @since 1.0.7
	 */
	public void setPhysicalCloseTimeout(int physicalCloseTimeout) {
		this.physicalCloseTimeout = Duration.ofSeconds(physicalCloseTimeout);
	}

	/**
	 * Set a prefix for the {@link ProducerConfig#TRANSACTIONAL_ID_CONFIG} config.
	 * By default a {@link ProducerConfig#TRANSACTIONAL_ID_CONFIG} value from configs is used as a prefix
	 * in the target producer configs.
	 *
	 * @param transactionIdPrefix the prefix.
	 * @since 1.3
	 */
	public final void setTransactionIdPrefix(String transactionIdPrefix) {
		Assert.notNull(transactionIdPrefix, "'transactionIdPrefix' cannot be null");
		this.transactionIdPrefix = transactionIdPrefix;
		enableIdempotentBehaviour();
	}

	protected String getTransactionIdPrefix() {
		return this.transactionIdPrefix;
	}

	/**
	 * When set to 'true', the producer will ensure that exactly one copy of each message is written in the stream.
	 */
	private void enableIdempotentBehaviour() {
		Object previousValue = this.configs.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
		if (Boolean.FALSE.equals(previousValue)) {
			LOGGER.debug(() -> "The '" + ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG
					+ "' is set to false, may result in duplicate messages");
		}
	}

	/**
	 * Set to false to revert to the previous behavior of a simple incrementing
	 * transactional.id suffix for each producer instead of maintaining a producer
	 * for each group/topic/partition.
	 *
	 * @param producerPerConsumerPartition false to revert.
	 * @since 1.3.7
	 */
	public void setProducerPerConsumerPartition(boolean producerPerConsumerPartition) {
		this.producerPerConsumerPartition = producerPerConsumerPartition;
	}

	/**
	 * Return the producerPerConsumerPartition.
	 *
	 * @return the producerPerConsumerPartition.
	 * @since 1.3.8
	 */
	@Override
	public boolean isProducerPerConsumerPartition() {
		return this.producerPerConsumerPartition;
	}

	/**
	 * Return an unmodifiable reference to the configuration map for this factory.
	 * Useful for cloning to make a similar factory.
	 *
	 * @return the configs.
	 * @since 1.3
	 */
	public Map<String, Object> getConfigurationProperties() {
		return Collections.unmodifiableMap(this.configs);
	}

	@Override
	public boolean transactionCapable() {
		return this.transactionIdPrefix != null;
	}

	@SuppressWarnings("resource")
	@Override
	public void destroy() {
		CloseSafeProducer<K, V> producerToClose = this.producer;
		this.producer = null;
		if (producerToClose != null) {
			producerToClose.delegate.close(this.physicalCloseTimeout);
		}
		producerToClose = this.cache.poll();
		while (producerToClose != null) {
			try {
				producerToClose.delegate.close(this.physicalCloseTimeout);
			} catch (Exception e) {
				LOGGER.error(e, "Exception while closing producer");
			}
			producerToClose = this.cache.poll();
		}
		synchronized (this.consumerProducers) {
			this.consumerProducers.forEach(
					(k, v) -> v.delegate.close(this.physicalCloseTimeout));
			this.consumerProducers.clear();
		}
	}

	@Override
	public void onApplicationEvent(ContextStoppedEvent event) {
		if (event.getApplicationContext().equals(this.applicationContext)) {
			reset();
		}
	}

	/**
	 * Close the {@link Producer}(s) and clear the cache of transactional
	 * {@link Producer}(s).
	 *
	 * @since 2.2
	 */
	public void reset() {
		try {
			destroy();
		} catch (Exception e) {
			LOGGER.error(e, "Exception while closing producer");
		}
	}

	/**
	 * NoOp.
	 *
	 * @return always true.
	 * @deprecated {@link org.springframework.context.Lifecycle} is no longer implemented.
	 */
	@Deprecated
	public boolean isRunning() {
		return true;
	}

	@Override
	public Producer<K, V> createProducer() {
		if (this.transactionIdPrefix != null) {
			if (this.producerPerConsumerPartition) {
				return createTransactionalProducerForPartition();
			} else {
				return createTransactionalProducer();
			}
		}
		if (this.producer == null) {
			synchronized (this) {
				if (this.producer == null) {
					this.producer = new CloseSafeProducer<>(createKafkaProducer());
				}
			}
		}
		return this.producer;
	}

	/**
	 * Subclasses must return a raw producer which will be wrapped in a
	 * {@link CloseSafeProducer}.
	 *
	 * @return the producer.
	 */
	protected Producer<K, V> createKafkaProducer() {
		return new KafkaProducer<>(this.configs, this.serializerFactory.getKeySerializer(), this.serializerFactory.getValueSerializer());
	}

	Producer<K, V> createTransactionalProducerForPartition() {
		String suffix = TransactionSupport.getTransactionIdSuffix();
		if (suffix == null) {
			return createTransactionalProducer();
		} else {
			synchronized (this.consumerProducers) {
				if (!this.consumerProducers.containsKey(suffix)) {
					CloseSafeProducer<K, V> newProducer = doCreateTxProducer(suffix, this::removeConsumerProducer);
					this.consumerProducers.put(suffix, newProducer);
					return newProducer;
				} else {
					return this.consumerProducers.get(suffix);
				}
			}
		}
	}

	private void removeConsumerProducer(CloseSafeProducer<K, V> producerToRemove) {
		synchronized (this.consumerProducers) {
			Iterator<Entry<String, CloseSafeProducer<K, V>>> iterator = this.consumerProducers.entrySet().iterator();
			while (iterator.hasNext()) {
				if (iterator.next().getValue().equals(producerToRemove)) {
					iterator.remove();
					break;
				}
			}
		}
	}

	/**
	 * Subclasses must return a producer from the {@link #getCache()} or a
	 * new raw producer wrapped in a {@link CloseSafeProducer}.
	 *
	 * @return the producer - cannot be null.
	 * @since 1.3
	 */
	protected Producer<K, V> createTransactionalProducer() {
		Producer<K, V> cachedProducer = this.cache.poll();
		if (cachedProducer == null) {
			return doCreateTxProducer("" + this.transactionIdSuffix.getAndIncrement(), null);
		} else {
			return cachedProducer;
		}
	}

	private CloseSafeProducer<K, V> doCreateTxProducer(String suffix,
													   @Nullable Consumer<CloseSafeProducer<K, V>> remover) {

		Producer<K, V> newProducer;
		Map<String, Object> newProducerConfigs = new HashMap<>(this.configs);
		newProducerConfigs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, this.transactionIdPrefix + suffix);
		newProducer = new KafkaProducer<>(newProducerConfigs, this.serializerFactory.getKeySerializer(), this.serializerFactory.getValueSerializer());
		newProducer.initTransactions();
		return new CloseSafeProducer<>(newProducer, this.cache, remover,
				(String) newProducerConfigs.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG));
	}

	protected BlockingQueue<CloseSafeProducer<K, V>> getCache() {
		return this.cache;
	}

	@Override
	public void closeProducerFor(String suffix) {
		if (this.producerPerConsumerPartition) {
			synchronized (this.consumerProducers) {
				CloseSafeProducer<K, V> removed = this.consumerProducers.remove(suffix);
				if (removed != null) {
					removed.delegate.close(this.physicalCloseTimeout);
				}
			}
		}
	}


	/**
	 * A wrapper class for the delegate.
	 *
	 * @param <K> the key type.
	 * @param <V> the value type.
	 */
	protected static class CloseSafeProducer<K, V> implements Producer<K, V> {

		private final Producer<K, V> delegate;

		private final BlockingQueue<CloseSafeProducer<K, V>> cache;

		private final Consumer<CloseSafeProducer<K, V>> removeConsumerProducer;

		private final String txId;

		private volatile boolean txFailed;

		CloseSafeProducer(Producer<K, V> delegate) {
			this(delegate, null, null);
			Assert.isTrue(!(delegate instanceof CloseSafeProducer), "Cannot double-wrap a producer");
		}

		CloseSafeProducer(Producer<K, V> delegate, BlockingQueue<CloseSafeProducer<K, V>> cache) {
			this(delegate, cache, null);
		}

		CloseSafeProducer(Producer<K, V> delegate, @Nullable BlockingQueue<CloseSafeProducer<K, V>> cache,
						  @Nullable Consumer<CloseSafeProducer<K, V>> removeConsumerProducer) {

			this(delegate, cache, removeConsumerProducer, null);
		}

		CloseSafeProducer(Producer<K, V> delegate, @Nullable BlockingQueue<CloseSafeProducer<K, V>> cache,
						  @Nullable Consumer<CloseSafeProducer<K, V>> removeConsumerProducer, @Nullable String txId) {

			this.delegate = delegate;
			this.cache = cache;
			this.removeConsumerProducer = removeConsumerProducer;
			this.txId = txId;
		}

		@Override
		public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
			LOGGER.trace(() -> toString() + " send(" + record + ")");
			return this.delegate.send(record);
		}

		@Override
		public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
			LOGGER.trace(() -> toString() + " send(" + record + ")");
			return this.delegate.send(record, callback);
		}

		@Override
		public void flush() {
			LOGGER.trace(() -> toString() + " flush()");
			this.delegate.flush();
		}

		@Override
		public List<PartitionInfo> partitionsFor(String topic) {
			return this.delegate.partitionsFor(topic);
		}

		@Override
		public Map<MetricName, ? extends Metric> metrics() {
			return this.delegate.metrics();
		}

		@Override
		public void initTransactions() {
			this.delegate.initTransactions();
		}

		@Override
		public void beginTransaction() throws ProducerFencedException {
			LOGGER.debug(() -> toString() + " beginTransaction()");
			try {
				this.delegate.beginTransaction();
			} catch (RuntimeException e) {
				LOGGER.error(e, () -> "beginTransaction failed: " + this);
				this.txFailed = true;
				throw e;
			}
		}

		@Override
		public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId)
				throws ProducerFencedException {

			LOGGER.trace(() -> toString() + " sendOffsetsToTransaction(" + offsets + ", " + consumerGroupId + ")");
			this.delegate.sendOffsetsToTransaction(offsets, consumerGroupId);
		}

		@Override
		public void commitTransaction() throws ProducerFencedException {
			LOGGER.debug(() -> toString() + " commitTransaction()");
			try {
				this.delegate.commitTransaction();
			} catch (RuntimeException e) {
				LOGGER.error(e, () -> "commitTransaction failed: " + this);
				this.txFailed = true;
				throw e;
			}
		}

		@Override
		public void abortTransaction() throws ProducerFencedException {
			LOGGER.debug(() -> toString() + " abortTransaction()");
			try {
				this.delegate.abortTransaction();
			} catch (RuntimeException e) {
				LOGGER.error(e, () -> "Abort failed: " + this);
				this.txFailed = true;
				throw e;
			}
		}

		@Override
		public void close() {
			close(null);
		}

		@Override
		@SuppressWarnings("deprecation")
		@Deprecated
		public void close(long timeout, @Nullable TimeUnit unit) {
			close(unit == null ? null : Duration.ofMillis(unit.toMillis(timeout)));
		}

		@Override
		public void close(@Nullable Duration timeout) {
			LOGGER.trace(() -> toString() + " close(" + (timeout == null ? "null" : timeout) + ")");
			if (this.cache != null) {
				if (this.txFailed) {
					LOGGER.warn(() -> "Error during transactional operation; producer removed from cache; "
							+ "possible cause: "
							+ "broker restarted during transaction: " + this);
					if (timeout == null) {
						this.delegate.close();
					} else {
						this.delegate.close(timeout);
					}
					if (this.removeConsumerProducer != null) {
						this.removeConsumerProducer.accept(this);
					}
				} else {
					if (this.removeConsumerProducer == null) { // dedicated consumer producers are not cached
						synchronized (this) {
							if (!this.cache.contains(this)
									&& !this.cache.offer(this)) {
								if (timeout == null) {
									this.delegate.close();
								} else {
									this.delegate.close(timeout);
								}
							}
						}
					}
				}
			}
		}

		@Override
		public String toString() {
			return "CloseSafeProducer [delegate=" + this.delegate + ""
					+ (this.txId != null ? ", txId=" + this.txId : "")
					+ "]";
		}

	}

}
