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

import org.apache.kafka.common.serialization.StringDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.BDDMockito.given
import org.mockito.Mockito

/**
 * @author Chris Gilbert
 */
class BeanLookupKafkaDeserializerFactoryTests {


    @Test
    fun `ensure consumer factory specific deserializer is retrieved`() {
        val target = TargetBeanLookupKafkDeserializerFactory()
        val expected = StringDeserializer()
        given(target.keyDeserializerFactory.getOrDefault("factory")).willReturn(expected)
        assertThat(target.getKeyDeserializer("factory")).isSameAs(expected)

    }

    @Test
    fun `ensure that all registered beans are returned correctly`() {
        val target = TargetBeanLookupKafkDeserializerFactory()
        given(target.keyDeserializerFactory.allMappedBeanNames).willReturn(setOf("deserializer1", "deserializer2"))
        given(target.valueDeserializerFactory.allMappedBeanNames).willReturn(setOf("deserializer2", "deserializer3"))
        assertThat(target.allRegisteredBeans).containsExactlyInAnyOrder("deserializer1", "deserializer2", "deserializer3")
    }

    class TargetBeanLookupKafkDeserializerFactory : BeanLookupKafkaDeserializerFactory<String, String>  {

        constructor() : super(null) {
            this.keyDeserializerFactory = mockGenericised()
            this.valueDeserializerFactory = mockGenericised()

        }

        inline fun <reified T: Any> mockGenericised() = Mockito.mock(T::class.java)
    }


}