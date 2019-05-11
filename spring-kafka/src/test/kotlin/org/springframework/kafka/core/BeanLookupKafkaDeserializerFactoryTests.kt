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
package org.springframework.kafka.core

import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.BDDMockito.given
import org.mockito.Mockito.mock
import org.springframework.beans.factory.NoSuchBeanDefinitionException
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory

/**
 * @author Chris Gilbert
 */
class BeanLookupKafkaDeserializerFactoryTests {


    @Test
    fun `ensure consumer factory specific deserializer is retrieved`() {
        val expected: Deserializer<String> = StringDeserializer()
        val beanFactory: ConfigurableListableBeanFactory = mock(ConfigurableListableBeanFactory::class.java)
        given(beanFactory.getBean("deserializer", Deserializer::class.java)).willReturn(expected)
        val target: BeanLookupKafkaDeserializerFactory<String, String> = BeanLookupKafkaDeserializerFactory()
        target.setBeanFactory(beanFactory)
        target.keyDeserializersForConsumerFactories["factory"] = "deserializer"
        assertThat(target.getKeyDeserializer("factory")).isSameAs(expected)

    }

    @Test
    fun `ensure default deserializer is retrieved when none exists for the specified consumer factory`() {
        val expected: Deserializer<String> = StringDeserializer()
        val beanFactory: ConfigurableListableBeanFactory = mock(ConfigurableListableBeanFactory::class.java)
        given(beanFactory.getBean("deserializer", Deserializer::class.java)).willReturn(expected)
        val target: BeanLookupKafkaDeserializerFactory<String, String> = BeanLookupKafkaDeserializerFactory()
        target.setBeanFactory(beanFactory)
        target.keyDeserializersForConsumerFactories["default"] = "deserializer"
        assertThat(target.getKeyDeserializer("factory")).isSameAs(expected)
    }

    @Test
    fun `ensure that more than one default deserializer cannot be registered`() {
        val target: BeanLookupKafkaDeserializerFactory<String, String> = BeanLookupKafkaDeserializerFactory()
        target.registerKeyDeserializer("deserializer")
        assertThrows<IllegalStateException> { target.registerKeyDeserializer("anotherDeserializer") }
    }

    @Test
    fun `ensure that more than one deserializer of the same type cannot be registered for the same consumer factory`() {
        val target: BeanLookupKafkaDeserializerFactory<String, String> = BeanLookupKafkaDeserializerFactory()
        target.registerKeyDeserializer("deserializer")
        assertThrows<IllegalStateException> { target.registerKeyDeserializer("anotherDeserializer") }
    }

    @Test
    fun `ensure that a deserializer cannot be registered if it is not known to the bean factory`() {
        val beanFactory: ConfigurableListableBeanFactory = mock(ConfigurableListableBeanFactory::class.java)
        given(beanFactory.getBeanDefinition("deserializer")).willThrow(NoSuchBeanDefinitionException(""))
        val target: BeanLookupKafkaDeserializerFactory<String, String> = BeanLookupKafkaDeserializerFactory()
        target.setBeanFactory(beanFactory)
        assertThrows<NoSuchBeanDefinitionException> { target.registerKeyDeserializer("deserializer") }
    }

    @Test
    fun `ensure that a deserializer cannot be registered to a consumer factory that is not known to the bean factory`() {
        val beanFactory: ConfigurableListableBeanFactory = mock(ConfigurableListableBeanFactory::class.java)
        given(beanFactory.getBeanDefinition("factory")).willThrow(NoSuchBeanDefinitionException(""))
        val target: BeanLookupKafkaDeserializerFactory<String, String> = BeanLookupKafkaDeserializerFactory()
        target.setBeanFactory(beanFactory)
        assertThrows<NoSuchBeanDefinitionException> { target.registerKeyDeserializer("factory", "deserializer") }
    }

    @Test
    fun `ensure that all registered beans are returned correctly`() {
        val target: BeanLookupKafkaDeserializerFactory<String, String> = BeanLookupKafkaDeserializerFactory()
        target.keyDeserializersForConsumerFactories = mapOf(Pair("default", "deserializer1"), Pair("factory", "deserializer1"))
        target.valueDeserializersForConsumerFactories = mapOf(Pair("default", "deserializer1"), Pair("factory", "deserializer2"))
        assertThat(target.allRegisteredBeans).containsExactly("deserializer1", "deserializer2")
    }
}