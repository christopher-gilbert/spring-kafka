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

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.annotation.ScannedGenericBeanDefinition;
import org.springframework.core.log.LogAccessor;
import org.springframework.core.type.StandardMethodMetadata;
import org.springframework.kafka.config.AbstractKafkaListenerContainerFactory;
import org.springframework.kafka.core.BeanLookupKafkaDeserializerFactory;
import org.springframework.kafka.core.KafkaConsumerFactoryWithDeserializerFactory;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * PostProcessor that provides any {@link KafkaConsumerFactoryWithDeserializerFactory} beans that have been added to
 * {@link org.springframework.kafka.config.KafkaListenerContainerFactory}s with a new instance of
 * {@link org.springframework.kafka.core.KafkaDeserializerFactory} that is populated based on
 * {@link KafkaKeyDeserializer} or {@link KafkaValueDeserializer} annotated beans of type {@link Deserializer}.
 *
 * @author Chris Gilbert
 */
public class KafkaSerializerAndDeserializerProcessor implements BeanPostProcessor, BeanFactoryAware, SmartInitializingSingleton {

	private BeanLookupKafkaDeserializerFactory deserializerFactory;

	private BeanFactory beanFactory;

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

	@Override
	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		return bean;
	}

	/**
	 * TODO documentation
	 * @param bean
	 * @param beanName
	 * @return
	 * @throws BeansException
	 */
	@Override
	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
		if (bean instanceof AbstractKafkaListenerContainerFactory) {
			AbstractKafkaListenerContainerFactory consumerFactoryOwner = (AbstractKafkaListenerContainerFactory) bean;
			if (consumerFactoryOwner.getConsumerFactory() instanceof KafkaConsumerFactoryWithDeserializerFactory) {
				KafkaConsumerFactoryWithDeserializerFactory consumerFactory =
						(KafkaConsumerFactoryWithDeserializerFactory) consumerFactoryOwner.getConsumerFactory();
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
		this.deserializerFactory = new BeanLookupKafkaDeserializerFactory(beanFactory);
	}

	/**
	 * TODO documentation and de-densify code
	 */
	@Override
	public void afterSingletonsInstantiated() {
		if (this.beanFactory instanceof ConfigurableListableBeanFactory) {
			String[] keyDeserializers = ((ConfigurableListableBeanFactory) this.beanFactory).getBeanNamesForType(Deserializer.class);
			for (String deserializerBean : keyDeserializers) {
				BeanDefinition definition = ((ConfigurableListableBeanFactory) this.beanFactory).getBeanDefinition(deserializerBean);
				if (definition.getSource() instanceof StandardMethodMetadata) {
					registerKeyDeserializer(deserializerBean, ((StandardMethodMetadata) definition.getSource())
							.getIntrospectedMethod().getAnnotation(KafkaKeyDeserializer.class));
					registerValueDeserializer(deserializerBean,	((StandardMethodMetadata) definition.getSource())
							.getIntrospectedMethod().getAnnotation(KafkaValueDeserializer.class));
				} else if (definition instanceof ScannedGenericBeanDefinition) {
					registerKeyDeserializer(deserializerBean, ((ScannedGenericBeanDefinition) definition)
							.getBeanClass().getAnnotation(KafkaKeyDeserializer.class));
					registerValueDeserializer(deserializerBean, ((ScannedGenericBeanDefinition) definition)
							.getBeanClass().getAnnotation(KafkaValueDeserializer.class));
				}
			}
		}
		checkForIncorrectlyAnnotatedBeans();
	}


	// TODO draw out commonality with registerValueDeserializer - not easy without annotation inheritance
	private void registerKeyDeserializer(String deserializerName, KafkaKeyDeserializer deserializerAnnotation) {
		if (deserializerAnnotation == null) {
			return;
		}
		if (deserializerAnnotation.consumerFactories().length == 0) {
			this.deserializerFactory.registerKeyDeserializer(deserializerName);
		} else {
			Arrays.stream(deserializerAnnotation.consumerFactories())
				  .forEach(factory -> this.deserializerFactory.registerKeyDeserializer(factory, deserializerName));
		}
	}

	private void registerValueDeserializer(String deserializerName, KafkaValueDeserializer deserializerAnnotation) {
		if (deserializerAnnotation == null) {
			return;
		}
		if (deserializerAnnotation.consumerFactories().length == 0) {
			this.deserializerFactory.registerValueDeserializer(deserializerName);
		} else {
			Arrays.stream(deserializerAnnotation.consumerFactories())
				  .forEach(factory -> this.deserializerFactory.registerValueDeserializer(factory, deserializerName));
		}
	}


	private void checkForIncorrectlyAnnotatedBeans() {
		if (this.beanFactory instanceof ConfigurableListableBeanFactory) {
			String[] keyDeserializers = ((ConfigurableListableBeanFactory) this.beanFactory)
					.getBeanNamesForAnnotation(KafkaKeyDeserializer.class);
			String[] valueDeserializers = ((ConfigurableListableBeanFactory) this.beanFactory)
					.getBeanNamesForAnnotation(KafkaValueDeserializer.class);
			Set<String> annotatedBeans = Stream.concat(Arrays.stream(keyDeserializers), Arrays.stream(valueDeserializers))
											   .collect(Collectors.toSet());
			annotatedBeans.removeAll(this.deserializerFactory.getAllRegisteredBeans());
			if (!annotatedBeans.isEmpty()) {
				logger.warn(() -> "Kafka Deserializer or Serializer annotations found on beans that are not of type Deserializer or Serializer" +
						" - these will be not be used, but will cause the beans to be prototype scope");
			}

		}
	}
}
