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

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.core.BeanLookupKafkaDeserializerFactory;
import org.springframework.kafka.core.KafkaConsumerFactoryWithDeserializerFactory;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation which may be applied to a {@link org.apache.kafka.common.serialization.Deserializer} bean definition
 * (either a {@link org.springframework.context.annotation.Bean} annotated method or a
 * {@link org.springframework.stereotype.Component} annotated type) which signifies that the bean is to be used as a
 * Deserializer for {@link org.apache.kafka.clients.consumer.ConsumerRecord} values received by a
 * {@link org.apache.kafka.clients.consumer.KafkaConsumer} from a
 * {@link KafkaConsumerFactoryWithDeserializerFactory} created with a
 * {@link BeanLookupKafkaDeserializerFactory} (this will be done automatically when you specify a
 * {@link KafkaConsumerFactoryWithDeserializerFactory} as the factory of a
 * {@link org.springframework.kafka.config.KafkaListenerContainerFactory} bean.
 * <p>
 * The optional consumerFactories property can be used in order to specify which consumerFactories to apply the
 * Deserializer to, otherwise it will be applied to any consumerFactories without a specific Deserializer.
 * <p>
 * The main driver for this annotation is to simplify the use of Deserializers with non-trivial instantiation
 * that cannot be shared between Consumers - sharing is problematic where the Deserializer {@link Deserializer#close()}
 * method is not a no-op, because the Deserializer lifecycle aligns with the lifecycle of individual consumer instances
 * (see {@link KafkaConsumer#close()}) and so any close actions on a Deserializer would impact other consumers using
 * it. Any bean annotated as a {@link KafkaValueDeserializer} will be a prototype scoped bean with each consumer
 * receiving a new instance.
 * <p>
 * If you need to use a separate {@link Deserializer} instance for each consumer, but the Deserializers can be created with
 * a no argument constructor, then use {@link org.springframework.kafka.core.DefaultKafkaConsumerFactory} and specify
 * the value deserializer class in application properties.
 * <p>
 * If you are using Deserializers with non-trivial instantiation that can be used even after being closed and so may be
 * safely shared, use {@link org.springframework.kafka.core.DefaultKafkaConsumerFactory} passing in a
 * {@link Deserializer} instance which will then be shared between all consumers.
 * <p>
 * For example usage, the following would apply the customDeserializer to values of records consumed by the
 * customContainerFactory's consumers, and the anotherCustomDeserializer to values of records consumed by
 * anotherCustomContainerFactory, and any other consumerFactories belonging to KafkaListenerContainerFactories
 * without specific Deserializers.
 * <p>
 * In both examples, the key deserializer has not been specified - either a keyDeserializer class should be
 * specified in application properties, else another Deserializer should be annotated with {@link KafkaKeyDeserializer}
 *
 * <pre class="code">
 *
 * &#064;Bean
 * public ConsumerFactory&lt;CustomKey, CustomEvent&gt; customConsumerFactory() {
 *   return new KafkaConsumerFactoryWithDeserializerFactory&lt;CustomKey, CustomEvent&gt;(kafkaProperties.buildConsumerProperties());
 * }
 *
 * &#064;Bean
 * &#064;KafkaValueDeserializer(consumerFactories = "customConsumerFactory")
 * public CustomDeserializer customDeserializer() {
 *   return new CustomDeserializer();
 * }
 *
 * &#064;Bean
 * &#064;KafkaValueDeserializer
 * public AnotherCustomDeserializer anotherCustomDeserializer() {
 *   return new AnotherCustomDeserializer();
 * }
 *
 * &#064;Bean
 * public KafkaListenerContainerFactory&lt;ConcurrentMessageListenerContainer&lt;CustomKey, CustomEvent&gt;&gt;customContainerFactory() {
 *     KafkaConsumerFactory&lt;CustomKey, CustomEvent&gt; consumerFactory = customConsumerFactory();
 *     KafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory&lt;CustomKey, CustomEvent&gt;();
 *     factory.consumerFactory = customConsumerFactory;
 *     factory.setConcurrency(5);
 *     return factory;
 * }
 *
 * &#064;Bean
 * public KafkaListenerContainerFactory&lt;ConcurrentMessageListenerContainer&lt;CustomKey, AnotherCustomEvent&gt;&gt;anotherCustomContainerFactory() {
 *     KafkaConsumerFactory&lt;CustomKey, AnotherCustomEvent&gt; consumerFactory = customConsumerFactory();
 *     KafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory&lt;CustomKey, AnotherCustomEvent&gt;();
 *     factory.consumerFactory = new KafkaConsumerFactoryWithDeserializerFactory&lt;CustomKey, AnotherCustomEvent&gt;(kafkaProperties.buildConsumerProperties());
 *     factory.setConcurrency(5);
 *     return factory;
 * }
 * </pre>
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
