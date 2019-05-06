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
import org.springframework.kafka.core.BeanLookupKafkaDeserializerFactory;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation which may be applied to a {@link org.apache.kafka.common.serialization.Deserializer} bean definition
 * (either a {@link org.springframework.context.annotation.Bean} annotated method or a
 * {@link org.springframework.stereotype.Component} annotated type) which signifies that the bean is to be used as a
 * Deserializer for {@link org.apache.kafka.clients.consumer.ConsumerRecord} values received by a
 * received by a {@link org.springframework.kafka.core.FactorySuppliedDeserializerKafkaConsumerFactory} created with an
 * {@link BeanLookupKafkaDeserializerFactory}.
 *
 * @author Chris Gilbert
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public @interface KafkaValueDeserializer {

	/**
	 * Bean names for specific ConsumerFactories for which the annotated Deserializer is applicable. If omitted then
	 * the Deserializer will apply to any ConsumerFactories that do not have a specific value deserializer.
	 *
	 * @return the array of ConsumerFactory bean names.
	 */
	String[] consumerFactories() default {};


}
