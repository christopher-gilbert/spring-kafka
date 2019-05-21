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

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation which may be applied to a {@link org.apache.kafka.common.serialization.Serializer} bean definition
 * (either a {@link org.springframework.context.annotation.Bean} annotated method or a
 * {@link org.springframework.stereotype.Component} annotated type) which signifies that the bean is to be used as a
 * Serializer for {@link org.apache.kafka.clients.producer.ProducerRecord} keys sent by a
 * {@link org.apache.kafka.clients.producer.KafkaProducer} from a
 * {@link org.springframework.kafka.core.FactorySuppliedSerializerKafkaProducerFactory} created with a
 * {@link BeanLookupKafkaSerializerFactory}.
 *
 * eg //TODO example config code
 *
 * @author Chris Gilbert
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public @interface KafkaKeySerializer {

	/**
	 * Bean names for specific ProducerFactories for which the annotated Deserializer is applicable. If omitted then
	 * the Serializer will apply to any ProducerFactories that do not have a specific value serializer.
	 *
	 * @return the array of ProducerFactory bean names.
	 */
	String[] producerFactories() default {};


}
