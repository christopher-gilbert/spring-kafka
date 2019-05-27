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
import org.apache.kafka.common.serialization.Serializer;
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
import org.springframework.kafka.core.*;

import java.lang.reflect.AnnotatedElement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * PostProcessor that provides any {@link KafkaConsumerFactoryWithDeserializerFactory} beans that have been added to
 * {@link org.springframework.kafka.config.KafkaListenerContainerFactory}s with a new instance of
 * {@link org.springframework.kafka.core.KafkaDeserializerFactory} that is populated based on
 * {@link KafkaKeyDeserializer} or {@link KafkaValueDeserializer} annotated beans of type {@link Deserializer}.
 * <p>
 * Also, carry out the same processing for {@link KafkaProducerFactoryWithSerializerFactory}
 *
 * @author Chris Gilbert
 */
public class KafkaSerializerAndDeserializerProcessor implements BeanPostProcessor, BeanFactoryAware, SmartInitializingSingleton {

	private BeanLookupKafkaDeserializerFactory<?, ?> deserializerFactory;

	private BeanLookupKafkaSerializerFactory<?, ?> serializerFactory;

	private BeanFactory beanFactory;

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

	@Override
	public Object postProcessBeforeInitialization(Object bean, String beanName) {
		return bean;
	}

	/**
	 * Inject consumer or producer factory beans with deserializer or serializer factories
	 * if they have not already had factories explicitly set.
	 *
	 * @param bean     the bean that owns the consumer or producer factory
	 * @param beanName the name of the above bean
	 * @return The bean
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Object postProcessAfterInitialization(Object bean, String beanName) {
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
		if (bean instanceof KafkaTemplate) {
			KafkaTemplate producerFactoryOwner = (KafkaTemplate) bean;
			if (KafkaProducerFactoryWithSerializerFactory.class.equals(producerFactoryOwner.getProducerFactory().getClass())) {
				KafkaProducerFactoryWithSerializerFactory producerFactory =
						(KafkaProducerFactoryWithSerializerFactory) producerFactoryOwner.getProducerFactory();
				if (!producerFactory.hasSerializerFactory()) {
					producerFactory.setSerializerFactory(serializerFactory);
				}
			}
		}
		return bean;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
		this.deserializerFactory = new BeanLookupKafkaDeserializerFactory(beanFactory);
		this.serializerFactory = new BeanLookupKafkaSerializerFactory(beanFactory);
	}

	/**
	 * Located annotated key and value serializers and deserializers, and register them with the appropriate
	 * factories. Also check for any annotations that have been applied to beans of the wrong type and warn
	 * as they would inherit prototype scope from the incorrectly applied annotation.
	 */
	@Override
	public void afterSingletonsInstantiated() {
		if (this.beanFactory instanceof ConfigurableListableBeanFactory) {
			annotatedBeanDeclarations(Deserializer.class).forEach(beanDefinition -> {
						registerKeyDeserializer(beanDefinition.getName(), beanDefinition.getDeclaration().getAnnotation(KafkaKeyDeserializer.class));
						registerValueDeserializer(beanDefinition.getName(), beanDefinition.getDeclaration().getAnnotation(KafkaValueDeserializer.class));
					}
			);
			annotatedBeanDeclarations(Serializer.class).forEach(beanDefinition -> {
						registerKeySerializer(beanDefinition.getName(), beanDefinition.getDeclaration().getAnnotation(KafkaKeySerializer.class));
						registerValueSerializer(beanDefinition.getName(), beanDefinition.getDeclaration().getAnnotation(KafkaValueSerializer.class));
					}
			);

			checkForIncorrectlyAnnotatedBeans();
		}

	}

	private List<BeanDefinitionHolder> annotatedBeanDeclarations(Class<?> type) {
		List<BeanDefinitionHolder> definitions = Arrays.stream(((ConfigurableListableBeanFactory) this.beanFactory).getBeanNamesForType(type))
													   .map(name -> new BeanDefinitionHolder(name, ((ConfigurableListableBeanFactory) this.beanFactory).getBeanDefinition(name)))
													   .collect(Collectors.toList());
		return Stream.concat(methodAnnotatedBeanDefinitions(definitions).stream(), classAnnotatedBeanDefinitions(definitions).stream())
					 .collect(Collectors.toList());

	}


	private List<BeanDefinitionHolder> methodAnnotatedBeanDefinitions(List<BeanDefinitionHolder> allDefinitions) {
		return allDefinitions.stream()
							 .filter(definition -> definition.getDefinition().getSource() instanceof StandardMethodMetadata)
							 .map(definition -> definition.withDeclaration(((StandardMethodMetadata) definition.getDefinition().getSource()).getIntrospectedMethod()))
							 .collect(Collectors.toList());

	}

	private List<BeanDefinitionHolder> classAnnotatedBeanDefinitions(List<BeanDefinitionHolder> allDefinitions) {
		return allDefinitions.stream()
							 .filter(definition -> definition.getDefinition() instanceof ScannedGenericBeanDefinition)
							 .map(definition -> definition.withDeclaration(((ScannedGenericBeanDefinition) definition.getDefinition()).getBeanClass()))
							 .collect(Collectors.toList());

	}

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

	private void registerKeySerializer(String serializerName, KafkaKeySerializer serializerAnnotation) {
		if (serializerAnnotation == null) {
			return;
		}
		if (serializerAnnotation.producerFactories().length == 0) {
			this.serializerFactory.registerKeySerializer(serializerName);
		} else {
			Arrays.stream(serializerAnnotation.producerFactories())
				  .forEach(factory -> this.serializerFactory.registerKeySerializer(factory, serializerName));
		}
	}

	private void registerValueSerializer(String serializerName, KafkaValueSerializer serializerAnnotation) {
		if (serializerAnnotation == null) {
			return;
		}
		if (serializerAnnotation.producerFactories().length == 0) {
			this.serializerFactory.registerValueSerializer(serializerName);
		} else {
			Arrays.stream(serializerAnnotation.producerFactories())
				  .forEach(factory -> this.serializerFactory.registerValueSerializer(factory, serializerName));
		}
	}


	private void checkForIncorrectlyAnnotatedBeans() {
		if (this.beanFactory instanceof ConfigurableListableBeanFactory) {
			ConfigurableListableBeanFactory clf = (ConfigurableListableBeanFactory) this.beanFactory;
			Set<String> annotatedBeans = new HashSet<>();
			annotatedBeans.addAll(Arrays.asList(clf.getBeanNamesForAnnotation(KafkaKeyDeserializer.class)));
			annotatedBeans.addAll(Arrays.asList(clf.getBeanNamesForAnnotation(KafkaValueDeserializer.class)));
			annotatedBeans.addAll(Arrays.asList(clf.getBeanNamesForAnnotation(KafkaKeySerializer.class)));
			annotatedBeans.addAll(Arrays.asList(clf.getBeanNamesForAnnotation(KafkaValueSerializer.class)));
			annotatedBeans.removeAll(this.deserializerFactory.getAllRegisteredBeans());
			annotatedBeans.removeAll(this.serializerFactory.getAllRegisteredBeans());
			if (!annotatedBeans.isEmpty()) {
				logger.warn(() -> "Kafka Deserializer or Serializer annotations found on beans named "
						+ String.join(",", annotatedBeans)
						+ " that are not of type Deserializer or Serializer"
						+ " - these will be not be used, but will cause the beans to be prototype scope");
			}

		}
	}

	/**
	 * Tuple required to hold a few different attributes of a bean definition during processing
	 */
	private static class BeanDefinitionHolder {
		private String name;
		private BeanDefinition definition;
		private AnnotatedElement declaration;

		BeanDefinitionHolder(String name, BeanDefinition definition) {
			this.name = name;
			this.definition = definition;
		}

		BeanDefinitionHolder(String name, BeanDefinition definition, AnnotatedElement declaration) {
			this.name = name;
			this.definition = definition;
			this.declaration = declaration;
		}

		BeanDefinitionHolder withDeclaration(AnnotatedElement declaration) {
			return new BeanDefinitionHolder(this.name, this.definition, declaration);
		}

		String getName() {
			return name;
		}

		BeanDefinition getDefinition() {
			return definition;
		}

		AnnotatedElement getDeclaration() {
			return declaration;
		}
	}

}
