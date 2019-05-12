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

import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link KafkaDeserializerFactory} implementation that retains a set of bean names and a {@link BeanFactory} reference
 * and uses these to retrieve bean instances whenever a {@link Deserializer} is requested. Hence each request for a
 * {@link Deserializer} may result in the same instance, or a new instance depending on whether the beans are declared
 * with Singleton scope or Prototype scope.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Chris Gilbert
 */
public class BeanLookupKafkaDeserializerFactory<K, V> implements KafkaDeserializerFactory<K, V> {

	protected MappedBeanFactory<Deserializer<K>> keyDeserializerFactory;

	protected MappedBeanFactory<Deserializer<V>> valueDeserializerFactory;

	public BeanLookupKafkaDeserializerFactory(BeanFactory beanFactory) {
		this.keyDeserializerFactory = new MappedBeanFactory<>(beanFactory);
		this.valueDeserializerFactory = new MappedBeanFactory<>(beanFactory);
	}

	/**
	 * Return a key {@link Deserializer} instance. If a specific {@link Deserializer} bean has been registered for the
	 * given consumer factory then that will be returned.
	 * <p>
	 * Otherwise if a {@link Deserializer} bean has been
	 * registered for any consumer factory then that will be returned.
	 * <p>
	 * If neither of the above is the case then null will be returned.
	 *
	 * @param consumerFactoryBeanName the name of the consumer factory that requires a key deserializer.
	 * @return the appropriate {@link Deserializer}, or null.
	 */
	@Override
	public Deserializer<K> getKeyDeserializer(String consumerFactoryBeanName) {
		return keyDeserializerFactory.getOrDefault(consumerFactoryBeanName);
	}

	/**
	 * Return a value {@link Deserializer} instance. If a specific {@link Deserializer} bean has been registered for the
	 * given consumer factory then that will be returned.
	 * <p>
	 * Otherwise if a {@link Deserializer} bean has been
	 * registered for any consumer factory then that will be returned.
	 * <p>
	 * If neither of the above is the case then null will be returned.
	 *
	 * @param consumerFactoryBeanName the name of the consumer factory that requires a value deserializer.
	 * @return the appropriate {@link Deserializer}, or null.
	 */
	@Override
	public Deserializer<V> getValueDeserializer(String consumerFactoryBeanName) {
		return valueDeserializerFactory.getOrDefault(consumerFactoryBeanName);
	}

	/**
	 * Register the given bean name as a key {@link Deserializer} that is suitable for any consumer created by a
	 * {@link KafkaConsumerFactoryWithDeserializerFactory} that does not have its own specific
	 * {@link Deserializer}.
	 *
	 * @param deserializerBeanName the name of the {@link Deserializer} bean
	 * @throws NoSuchBeanDefinitionException if there is no bean with this name in the current application.
	 * @throws IllegalStateException         if there is already a default key deserializer bean registered.
	 */
	public void registerKeyDeserializer(String deserializerBeanName) {
		keyDeserializerFactory.addDefaultBeanMapping(deserializerBeanName);
	}

	/**
	 * Register the given bean name as a key {@link Deserializer} that is suitable for any consumer created by the
	 * {@link KafkaConsumerFactoryWithDeserializerFactory} with the given factory bean name.
	 *
	 * @param consumerFactoryBeanName the name of the {@link KafkaConsumerFactoryWithDeserializerFactory}
	 * @param deserializerBeanName    the name of the {@link Deserializer} bean
	 * @throws NoSuchBeanDefinitionException if either of the provided bean names do not match beans in the current
	 *                                       application.
	 * @throws IllegalStateException         if there is already a key deserializer bean registered for the given consumer factory.
	 */
	public void registerKeyDeserializer(String consumerFactoryBeanName, String deserializerBeanName) {
		keyDeserializerFactory.addBeanMapping(consumerFactoryBeanName, deserializerBeanName);
	}

	/**
	 * Register the given bean name as a value {@link Deserializer} that is suitable for any consumer created by a
	 * {@link KafkaConsumerFactoryWithDeserializerFactory} that does not have its own specific
	 * {@link Deserializer}.
	 *
	 * @param deserializerBeanName the name of the {@link Deserializer} bean
	 * @throws NoSuchBeanDefinitionException  if there is no bean with this name in the current application.
	 * @throws IllegalStateException          if there is already a default value deserializer bean registered.
	 */
	public void registerValueDeserializer(String deserializerBeanName) {
		valueDeserializerFactory.addDefaultBeanMapping(deserializerBeanName);
	}

	/**
	 * Register the given bean name as a value {@link Deserializer} that is suitable for any consumer created by the
	 * {@link KafkaConsumerFactoryWithDeserializerFactory} with the given factory bean name.
	 *
	 * @param consumerFactoryBeanName the name of the {@link KafkaConsumerFactoryWithDeserializerFactory}
	 * @param deserializerBeanName    the name of the {@link Deserializer} bean
	 * @throws NoSuchBeanDefinitionException if either of the provided bean names do not match beans in the current
	 *                                       application.
	 * @throws IllegalStateException         if there is already a value deserializer bean registered for the given consumer factory.
	 */
	public void registerValueDeserializer(String consumerFactoryBeanName, String deserializerBeanName) {
		valueDeserializerFactory.addBeanMapping(consumerFactoryBeanName, deserializerBeanName);
	}

	/**
	 * Return a set of all the deserializer beans that have been registered against any consumer factories as
	 * key or value deserializers.
	 *
	 * @return the set of bean names
	 */
	public Collection<String> getAllRegisteredBeans() {
		return Stream.concat(keyDeserializerFactory.getAllMappedBeanNames().stream(), valueDeserializerFactory.getAllMappedBeanNames().stream())
					 .collect(Collectors.toSet());
	}


}
