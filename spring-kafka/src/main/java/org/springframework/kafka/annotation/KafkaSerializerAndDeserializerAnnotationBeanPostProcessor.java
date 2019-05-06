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
package org.springframework.kafka.annotation;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.kafka.config.AbstractKafkaListenerContainerFactory;
import org.springframework.kafka.core.BeanLookupKafkaDeserializerFactory;
import org.springframework.kafka.core.FactorySuppliedDeserializerKafkaConsumerFactory;

/**
 * PostProcessor that provides any {@link FactorySuppliedDeserializerKafkaConsumerFactory}s that have been added to
 * {@link org.springframework.kafka.config.KafkaListenerContainerFactory}s with a
 * {@link org.springframework.kafka.core.KafkaDeserializerFactory} that is populated based on annotated Deserializer
 * beans.
 *
 * @author Chris Gilbert
 */
public class KafkaSerializerAndDeserializerAnnotationBeanPostProcessor implements BeanPostProcessor, BeanFactoryAware {

	private BeanLookupKafkaDeserializerFactory deserializerFactory = new BeanLookupKafkaDeserializerFactory();

	private BeanFactory beanFactory;

	@Override
	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		return bean;
	}

	@Override
	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
		if (bean instanceof AbstractKafkaListenerContainerFactory) {
			AbstractKafkaListenerContainerFactory consumerFactoryOwner = (AbstractKafkaListenerContainerFactory) bean;
			if (consumerFactoryOwner.getConsumerFactory() instanceof FactorySuppliedDeserializerKafkaConsumerFactory) {
				FactorySuppliedDeserializerKafkaConsumerFactory consumerFactory = (FactorySuppliedDeserializerKafkaConsumerFactory) consumerFactoryOwner.getConsumerFactory();
				if (!consumerFactory.hasDeserializerFactory()) {
					consumerFactory.setDeserializerFactory(deserializerFactory);
				}
			}
		}
		return bean;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}

	// TODO implement a lifecycle hook that uses the bean factory to locate bean definitions that include
	// key or value deserializer annotations, and registers them with the deserializer factory
}
