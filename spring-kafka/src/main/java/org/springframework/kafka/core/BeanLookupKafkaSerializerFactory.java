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
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link KafkaSerializerFactory} implementation that retains a set of bean names and a {@link BeanFactory} reference
 * and uses these to retrieve bean instances whenever a {@link Serializer} is requested. Hence each request for a
 * {@link Serializer} may result in the same instance, or a new instance depending on whether the beans are declared
 * with Singleton scope or Prototype scope.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Chris Gilbert
 */
public class BeanLookupKafkaSerializerFactory<K, V> implements KafkaSerializerFactory<K, V> {

	protected MappedBeanFactory<Serializer<K>> keySerializerFactory;

	protected MappedBeanFactory<Serializer<V>> valueSerializerFactory;

	public BeanLookupKafkaSerializerFactory(BeanFactory beanFactory) {
		this.keySerializerFactory = new MappedBeanFactory<>(beanFactory);
		this.valueSerializerFactory = new MappedBeanFactory<>(beanFactory);
	}

	/**
	 * Return a key {@link Serializer} instance. If a specific {@link Serializer} bean has been registered for the
	 * given consumer factory then that will be returned.
	 * <p>
	 * Otherwise if a {@link Serializer} bean has been
	 * registered for any consumer factory then that will be returned.
	 * <p>
	 * If neither of the above is the case then null will be returned.
	 *
	 * @param consumerFactoryBeanName the name of the consumer factory that requires a key serializer.
	 * @return the appropriate {@link Serializer}, or null.
	 */
	@Override
	public Serializer<K> getKeySerializer(String consumerFactoryBeanName) {
		return keySerializerFactory.getOrDefault(consumerFactoryBeanName);
	}

	/**
	 * Return a value {@link Serializer} instance. If a specific {@link Serializer} bean has been registered for the
	 * given consumer factory then that will be returned.
	 * <p>
	 * Otherwise if a {@link Serializer} bean has been
	 * registered for any consumer factory then that will be returned.
	 * <p>
	 * If neither of the above is the case then null will be returned.
	 *
	 * @param consumerFactoryBeanName the name of the consumer factory that requires a value serializer.
	 * @return the appropriate {@link Serializer}, or null.
	 */
	@Override
	public Serializer<V> getValueSerializer(String consumerFactoryBeanName) {
		return valueSerializerFactory.getOrDefault(consumerFactoryBeanName);
	}

	/**
	 * Register the given bean name as a key {@link Serializer} that is suitable for any producer created by a
	 * {@link KafkaProducerFactoryWithSerializerFactory} that does not have its own specific
	 * {@link Serializer}.
	 *
	 * @param serializerBeanName the name of the {@link Serializer} bean
	 * @throws NoSuchBeanDefinitionException if there is no bean with this name in the current application.
	 * @throws IllegalStateException         if there is already a default key serializer bean registered.
	 */
	public void registerKeySerializer(String serializerBeanName) {
		keySerializerFactory.addDefaultBeanMapping(serializerBeanName);
	}

	/**
	 * Register the given bean name as a key {@link Serializer} that is suitable for any producer created by the
	 * {@link KafkaProducerFactoryWithSerializerFactory} with the given factory bean name.
	 *
	 * @param producerFactoryBeanName the name of the {@link KafkaProducerFactoryWithSerializerFactory}
	 * @param serializerBeanName    the name of the {@link Serializer} bean
	 * @throws NoSuchBeanDefinitionException if either of the provided bean names do not match beans in the current
	 *                                       application.
	 * @throws IllegalStateException         if there is already a key serializer bean registered for the given producer factory.
	 */
	public void registerKeySerializer(String producerFactoryBeanName, String serializerBeanName) {
		keySerializerFactory.addBeanMapping(producerFactoryBeanName, serializerBeanName);
	}

	/**
	 * Register the given bean name as a value {@link Serializer} that is suitable for any producer created by a
	 * {@link KafkaProducerFactoryWithSerializerFactory} that does not have its own specific
	 * {@link Serializer}.
	 *
	 * @param serializerBeanName the name of the {@link Serializer} bean
	 * @throws NoSuchBeanDefinitionException  if there is no bean with this name in the current application.
	 * @throws IllegalStateException          if there is already a default value serializer bean registered.
	 */
	public void registerValueSerializer(String serializerBeanName) {
		valueSerializerFactory.addDefaultBeanMapping(serializerBeanName);
	}

	/**
	 * Register the given bean name as a value {@link Serializer} that is suitable for any producer created by the
	 * {@link KafkaProducerFactoryWithSerializerFactory} with the given factory bean name.
	 *
	 * @param producerFactoryBeanName the name of the {@link KafkaProducerFactoryWithSerializerFactory}
	 * @param serializerBeanName    the name of the {@link Serializer} bean
	 * @throws NoSuchBeanDefinitionException if either of the provided bean names do not match beans in the current
	 *                                       application.
	 * @throws IllegalStateException         if there is already a value serializer bean registered for the given producer factory.
	 */
	public void registerValueSerializer(String producerFactoryBeanName, String serializerBeanName) {
		valueSerializerFactory.addBeanMapping(producerFactoryBeanName, serializerBeanName);
	}

	/**
	 * Return a set of all the serializer beans that have been registered against any consumer factories as
	 * key or value serializers.
	 *
	 * @return the set of bean names
	 */
	public Collection<String> getAllRegisteredBeans() {
		return Stream.concat(keySerializerFactory.getAllMappedBeanNames().stream(), valueSerializerFactory.getAllMappedBeanNames().stream())
					 .collect(Collectors.toSet());
	}


}
