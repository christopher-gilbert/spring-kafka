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

import org.apache.kafka.common.serialization.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.BeanFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.*
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.stereotype.Component
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig
import javax.annotation.Resource

/**
 * @author Chris Gilbert
 */
@SpringJUnitConfig
@DirtiesContext
class KafkaSerializerAndDeserializerProcessorIntegrationTests {

    @Resource(name = "consumerFactory")
    private lateinit var consumerFactory: KafkaConsumerFactoryWithDeserializerFactory<String, String>

    @Resource(name = "consumerFactoryWithProvidedDeserializerFactory")
    private lateinit var consumerFactoryWithProvidedDeserializerFactory: KafkaConsumerFactoryWithDeserializerFactory<String, String>

    @Autowired
    private lateinit var providedDeserializerFactory: KafkaDeserializerFactory<String, String>

    @Resource(name = "producerFactory")
    private lateinit var producerFactory: KafkaProducerFactoryWithSerializerFactory<String, String>

    @Resource(name = "producerFactoryWithProvidedSerializerFactory")
    private lateinit var producerFactoryWithProvidedSerializerFactory: KafkaProducerFactoryWithSerializerFactory<String, String>

    @Autowired
    private lateinit var providedSerializerFactory: KafkaSerializerFactory<String, String>

    @Test
    fun `ensure deserializer factory is injected into consumer factory by default`() {
        assertThat(this.consumerFactory.hasDeserializerFactory()).isTrue()
        assertThat(this.consumerFactory.deserializerFactory)
                .isInstanceOf(BeanLookupKafkaDeserializerFactory::class.java)
    }

    @Test
    fun `ensure explicitly provided deserializer factory is not replaced`() {
        assertThat(this.consumerFactoryWithProvidedDeserializerFactory.hasDeserializerFactory()).isTrue()
        assertThat(this.consumerFactoryWithProvidedDeserializerFactory.deserializerFactory).isSameAs(providedDeserializerFactory)
    }

    @Test
    fun `ensure annotated deserializer without a named consumer factory is added to deserializer factory as default`() {
        assertThat(this.consumerFactory.deserializerFactory.getValueDeserializer("consumerFactory2")).isInstanceOf(ComponentDeserializer::class.java)
        assertThat(this.consumerFactory.deserializerFactory.getValueDeserializer("consumerFactory3")).isInstanceOf(ComponentDeserializer::class.java)

    }

    @Test
    fun `ensure annotated deserializer with a named consumer factory is added to deserializer factory for that consumer`() {
        assertThat(this.consumerFactory.deserializerFactory
                .getKeyDeserializer("consumerFactory"))
                .isInstanceOf(StringDeserializer::class.java)

    }

    @Test
    fun `ensure bean that is key and value deserializer is registered as both`() {
        assertThat(this.consumerFactory.deserializerFactory
                .getKeyDeserializer("consumerFactory4"))
                .isEqualTo(this.consumerFactory.deserializerFactory
                        .getValueDeserializer("consumerFactory4"))
                .isInstanceOf(KeyAndValueDeserializer::class.java)
    }

    @Test
    fun `ensure deserializers retrieved from the factory are prototypes`() {
        assertThat(this.consumerFactory.deserializerFactory.getKeyDeserializer("consumerFactory"))
                .isNotSameAs(this.consumerFactory.deserializerFactory.getKeyDeserializer("consumerFactory"))
    }

    @Test
    fun `ensure serializer factory is injected into producer factory by default`() {
        assertThat(this.producerFactory.hasSerializerFactory()).isTrue()
        assertThat(this.producerFactory.serializerFactory)
                .isInstanceOf(BeanLookupKafkaSerializerFactory::class.java)
    }

    @Test
    fun `ensure explicitly provided serializer factory is not replaced`() {
        assertThat(this.producerFactoryWithProvidedSerializerFactory.hasSerializerFactory()).isTrue()
        assertThat(this.producerFactoryWithProvidedSerializerFactory.serializerFactory).isSameAs(providedSerializerFactory)
    }

    @Test
    fun `ensure annotated serializer without a named producer factory is added to serializer factory as default`() {
        assertThat(this.producerFactory.serializerFactory.getValueSerializer("producerFactory2")).isInstanceOf(ComponentSerializer::class.java)
        assertThat(this.producerFactory.serializerFactory.getValueSerializer("producerFactory3")).isInstanceOf(ComponentSerializer::class.java)

    }

    @Test
    fun `ensure annotated serializer with a named producer factory is added to serializer factory for that consumer`() {
        assertThat(this.producerFactory.serializerFactory
                .getKeySerializer("producerFactory"))
                .isInstanceOf(StringSerializer::class.java)

    }

    @Test
    fun `ensure bean that is key and value serializer is registered as both`() {
        assertThat(this.producerFactory.serializerFactory
                .getKeySerializer("producerFactory4"))
                .isEqualTo(this.producerFactory.serializerFactory
                        .getValueSerializer("producerFactory4"))
                .isInstanceOf(KeyAndValueSerializer::class.java)
    }

    @Test
    fun `ensure serializers retrieved from the factory are prototypes`() {
        assertThat(this.producerFactory.serializerFactory.getKeySerializer("producerFactory"))
                .isNotSameAs(this.producerFactory.serializerFactory.getKeySerializer("producerFactory"))
    }

    @Configuration
    @ComponentScan("org.springframework.kafka.core")
    @EnableKafka
    class Config {

        @Autowired
        private lateinit var beanFactory: BeanFactory

        // consumer config

        @Bean
        fun consumerFactory(): KafkaConsumerFactoryWithDeserializerFactory<String, String>
                = KafkaConsumerFactoryWithDeserializerFactory(HashMap<String, Any>())

        @Bean
        fun consumerFactory2(): KafkaConsumerFactoryWithDeserializerFactory<String, String>
                = KafkaConsumerFactoryWithDeserializerFactory(HashMap<String, Any>())

        @Bean
        fun consumerFactory3(): KafkaConsumerFactoryWithDeserializerFactory<String, String>
                = KafkaConsumerFactoryWithDeserializerFactory(HashMap<String, Any>())

        @Bean
        fun consumerFactory4(): KafkaConsumerFactoryWithDeserializerFactory<String, String>
                = KafkaConsumerFactoryWithDeserializerFactory(HashMap<String, Any>())

        @Bean
        fun deserializerFactory(): KafkaDeserializerFactory<String, String>
                = BeanLookupKafkaDeserializerFactory<String, String>(this.beanFactory)

        @Bean
        fun consumerFactoryWithProvidedDeserializerFactory(): KafkaConsumerFactoryWithDeserializerFactory<String, String> {
            val factory: KafkaConsumerFactoryWithDeserializerFactory<String, String>
                    = KafkaConsumerFactoryWithDeserializerFactory(HashMap<String, Any>())
            factory.deserializerFactory = deserializerFactory()
            return factory
        }

        @Bean
        fun kafkaListenerContainerFactory1(): ConcurrentKafkaListenerContainerFactory<String, String> {
            val factory: ConcurrentKafkaListenerContainerFactory<String, String> = ConcurrentKafkaListenerContainerFactory()
            factory.consumerFactory = consumerFactory()
            return factory
        }

        @Bean
        fun kafkaListenerContainerFactory2(): ConcurrentKafkaListenerContainerFactory<String, String> {
            val factory: ConcurrentKafkaListenerContainerFactory<String, String> = ConcurrentKafkaListenerContainerFactory()
            factory.consumerFactory = consumerFactoryWithProvidedDeserializerFactory()
            return factory
        }

        @Bean
        fun kafkaListenerContainerFactory3(): ConcurrentKafkaListenerContainerFactory<String, String> {
            val factory: ConcurrentKafkaListenerContainerFactory<String, String> = ConcurrentKafkaListenerContainerFactory()
            factory.consumerFactory = consumerFactory4()
            return factory
        }

        @Bean
        @KafkaKeyDeserializer(consumerFactories = ["consumerFactory"])
        fun configurationDeserializer(): Deserializer<String> = StringDeserializer()


        @Bean
        @KafkaKeyDeserializer(consumerFactories = ["consumerFactory4"])
        @KafkaValueDeserializer(consumerFactories = ["consumerFactory4"])
        fun keyAndValueDeserializer(): Deserializer<String> = KeyAndValueDeserializer("test")

        // producer config

        @Bean
        fun producerFactory(): KafkaProducerFactoryWithSerializerFactory<String, String>
                = KafkaProducerFactoryWithSerializerFactory(HashMap<String, Any>())

        @Bean
        fun producerFactory2(): KafkaProducerFactoryWithSerializerFactory<String, String>
                = KafkaProducerFactoryWithSerializerFactory(HashMap<String, Any>())

        @Bean
        fun producerFactory3(): KafkaProducerFactoryWithSerializerFactory<String, String>
                = KafkaProducerFactoryWithSerializerFactory(HashMap<String, Any>())

        @Bean
        fun producerFactory4(): KafkaProducerFactoryWithSerializerFactory<String, String>
                = KafkaProducerFactoryWithSerializerFactory(HashMap<String, Any>())

        @Bean
        fun serializerFactory(): KafkaSerializerFactory<String, String>
                = BeanLookupKafkaSerializerFactory<String, String>(this.beanFactory)

        @Bean
        fun producerFactoryWithProvidedSerializerFactory(): KafkaProducerFactoryWithSerializerFactory<String, String> {
            val factory: KafkaProducerFactoryWithSerializerFactory<String, String>
                    = KafkaProducerFactoryWithSerializerFactory(HashMap<String, Any>())
            factory.serializerFactory = serializerFactory()
            return factory
        }

        @Bean
        fun kafkaTemplate1(): KafkaTemplate<String, String> = KafkaTemplate(producerFactory())

        @Bean
        fun kafkaTemplate2(): KafkaTemplate<String, String> = KafkaTemplate(producerFactoryWithProvidedSerializerFactory())

        @Bean
        fun kafkaTemplate3(): KafkaTemplate<String, String> = KafkaTemplate(producerFactory4())

        @Bean
        @KafkaKeySerializer(producerFactories = ["producerFactory"])
        fun configurationSerializer(): Serializer<String> = StringSerializer()


        @Bean
        @KafkaKeySerializer(producerFactories = ["producerFactory4"])
        @KafkaValueSerializer(producerFactories = ["producerFactory4"])
        fun keyAndValueSerializer(): Serializer<String> = KeyAndValueSerializer("test")
    }

    @Component
    @KafkaValueDeserializer
    class ComponentDeserializer : IntegerDeserializer()


    @Component
    @KafkaValueSerializer
    class ComponentSerializer : IntegerSerializer()

    data class KeyAndValueDeserializer(val field: String) : StringDeserializer()

    data class KeyAndValueSerializer(val field: String) : StringSerializer()

}
