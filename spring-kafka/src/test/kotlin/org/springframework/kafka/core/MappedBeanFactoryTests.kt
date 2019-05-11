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
package org.springframework.kafka.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.BDDMockito.given
import org.mockito.Mockito.mock
import org.springframework.beans.factory.BeanFactory
import org.springframework.beans.factory.NoSuchBeanDefinitionException
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory

/**
 * @author Chris Gilbert
 */
class MappedBeanFactoryTests {


    @Test
    fun `ensure specific bean is retrieved`() {
        val expected = Any()
        val beanFactory: BeanFactory = mock(BeanFactory::class.java)
        given(beanFactory.getBean("testValue")).willReturn(expected)
        val target: MappedBeanFactory<Any> = MappedBeanFactory(beanFactory)
        target.addBeanMapping("testKey", "testValue")
        assertThat(target.getOrDefault("testKey")).isSameAs(expected)

    }

    @Test
    fun `ensure default bean is retrieved when none exists for the specified key`() {
        val expected = Any()
        val beanFactory: BeanFactory = mock(BeanFactory::class.java)
        given(beanFactory.getBean("testValue")).willReturn(expected)
        val target: MappedBeanFactory<Any> = MappedBeanFactory(beanFactory)
        target.addDefaultBeanMapping("testValue")
        assertThat(target.getOrDefault("unregisteredKey")).isSameAs(expected)
    }

    @Test
    fun `ensure that more than one default bean cannot be registered`() {
        val target: MappedBeanFactory<Any> = MappedBeanFactory(mock(BeanFactory::class.java))
        target.addDefaultBeanMapping("testValue")
        assertThrows<IllegalStateException> { target.addDefaultBeanMapping("anotherTestValue") }
    }

    @Test
    fun `ensure that more than one bean of the same type cannot be registered for the same key`() {
        val target: MappedBeanFactory<Any> = MappedBeanFactory(mock(BeanFactory::class.java))
        target.addBeanMapping("testKey", "testValue")
        assertThrows<IllegalStateException> { target.addBeanMapping("testKey", "anotherTestValue") }
    }

    @Test
    fun `ensure that a bean cannot be registered if it is not known to the bean factory`() {
        val beanFactory: ConfigurableListableBeanFactory = mock(ConfigurableListableBeanFactory::class.java)
        given(beanFactory.getBeanDefinition("testValue")).willThrow(NoSuchBeanDefinitionException(""))
        val target: MappedBeanFactory<Any> = MappedBeanFactory(beanFactory)
        assertThrows<NoSuchBeanDefinitionException> { target.addDefaultBeanMapping("testValue") }
    }

    @Test
    fun `ensure that a bean cannot be registered to a key that is not known to the bean factory`() {
        val beanFactory: ConfigurableListableBeanFactory = mock(ConfigurableListableBeanFactory::class.java)
        given(beanFactory.getBeanDefinition("testKey")).willThrow(NoSuchBeanDefinitionException(""))
        val target: MappedBeanFactory<Any> = MappedBeanFactory(beanFactory)
        assertThrows<NoSuchBeanDefinitionException> { target.addBeanMapping("testKey", "testValue") }
    }

    @Test
    fun `ensure that all registered beans are returned correctly`() {
        val target: MappedBeanFactory<Any> = MappedBeanFactory(mock(BeanFactory::class.java))
        target.addDefaultBeanMapping("testValue1")
        target.addBeanMapping("testKey1", "testValue1")
        target.addBeanMapping("testKey2", "testValue2")
        assertThat(target.allMappedBeanNames).containsExactlyInAnyOrder("testValue1", "testValue2")
    }
}