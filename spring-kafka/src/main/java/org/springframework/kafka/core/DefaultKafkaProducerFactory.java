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

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;

import java.util.Map;

/**
 * {@link ProducerFactory} that makes use of a {@link KafkaSerializerFactory} which always provides the same instance of
 * key and value serializer for each producer it creates, if provided. If none is provided but serializer classes
 * are specified in application properties then new instances of those classes are created for each producer.
 * In most cases these options are all you will need, unless ALL the following apply:
 * <ul>
 *     <li>
 *         There is more than one producer factory in the JVM using the same serializers.
 *     </li>
 *     <li>
 *          The serializers either do not have a no argument constructor, or they require some initialization which
 *          means they cannot be specified in application properties.
 *      </li>
 *      <li>
 *          The serializers perform some action in their close method that renders them unusable subsequently
 *      </li>
 * </ul>
 * In this case, you should use {@link KafkaProducerFactoryWithSerializerFactory} and either provide your own
 * factory implementation, or alternatively a {@link BeanLookupKafkaSerializerFactory} is implicitly created and
 * populated with any serializers annotated as {@link org.springframework.kafka.annotation.KafkaKeySerializer} or
 * {@link org.springframework.kafka.annotation.KafkaValueSerializer}
 *
 * @param <K> the key type in produced {@link org.apache.kafka.clients.producer.ProducerRecord}s
 * @param <V> the value type in consumed {@link org.apache.kafka.clients.producer.ProducerRecord}s
 * @author Chris Gilbert
 */
public class DefaultKafkaProducerFactory<K, V> extends KafkaProducerFactoryWithSerializerFactory<K, V> {

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(KafkaProducerFactoryWithSerializerFactory.class));


	/**
	 * Construct a factory with the provided configuration.
	 *
	 * @param configs the configuration.
	 */
	public DefaultKafkaProducerFactory(Map<String, Object> configs) {
		this(configs, null, null);
	}

	/**
	 * Construct a factory with the provided configuration and {@link Serializer}s.
	 * 	 *
	 * @param configs         the configuration.
	 * @param keySerializer   the key {@link Serializer}.
	 * @param valueSerializer the value {@link Serializer}.
	 */
	public DefaultKafkaProducerFactory(Map<String, Object> configs,
									   @Nullable Serializer<K> keySerializer,
									   @Nullable Serializer<V> valueSerializer) {
		super(configs, new SingleInstanceKafkaSerializerFactory(keySerializer, valueSerializer));
	}


	public void setKeySerializer(@Nullable Serializer<K> keySerializer) {
		if (super.getSerializerFactory() instanceof SingleInstanceKafkaSerializerFactory) {
			((SingleInstanceKafkaSerializerFactory) super.getSerializerFactory()).setKeySerializer(keySerializer);
		}
		//TODO else warn?
	}

	public void setValueSerializer(@Nullable Serializer<V> valueSerializer) {
		if (super.getSerializerFactory() instanceof SingleInstanceKafkaSerializerFactory) {
			((SingleInstanceKafkaSerializerFactory) super.getSerializerFactory()).setValueSerializer(valueSerializer);
		}
		//TODO else warn?
	}


	/**
	 * Simple implementation of {@link KafkaSerializerFactory} that provides the same {@link Serializer} instances
	 * every time UNLESS the {@link Serializer} instance variables are modified between calls. These fields are only
	 * mutable in order to honour the public mutators in {@link DefaultKafkaProducerFactory}, and are not expected
	 * to be modified after initial creation.
	 */
	private static class SingleInstanceKafkaSerializerFactory implements KafkaSerializerFactory {

		private Serializer keySerializer;

		private Serializer valueSerializer;

		public SingleInstanceKafkaSerializerFactory(Serializer keySerializer, Serializer valueSerializer) {
			this.keySerializer = keySerializer;
			this.valueSerializer = valueSerializer;
		}

		@Override
		public Serializer getKeySerializer() {
			return keySerializer;
		}

		@Override
		public Serializer getValueSerializer() {
			return valueSerializer;
		}

		public void setKeySerializer(Serializer keySerializer) {
			this.keySerializer = keySerializer;
		}

		public void setValueSerializer(Serializer valueSerializer) {
			this.valueSerializer = valueSerializer;
		}

	}

}
