package org.springframework.kafka.core

import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.KafkaKeyDeserializer
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.stereotype.Component
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig
import javax.annotation.Resource

@SpringJUnitConfig
@DirtiesContext
class KafkaSerializerAndDeserializerAnnotationBeanPostProcessorIntegrationTests {

    @Resource(name = "consumerFactory")
    private lateinit var consumerFactory: FactorySuppliedDeserializerKafkaConsumerFactory<String, String>

    @Resource(name = "consumerFactoryWithProvidedDeserializerFactory")
    private lateinit var consumerFactoryWithProvidedDeserializerFactory: FactorySuppliedDeserializerKafkaConsumerFactory<String, String>

    @Autowired
    private lateinit var providedDeserializerFactory: KafkaDeserializerFactory<String, String>

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
    fun `ensure annotated deserializer without a named consumer is added to factory as default`() {

    }

    @Test
    fun `ensure annotated deserializer with a named consumer is added to factory for that consumer`() {
        assertThat(this.consumerFactory.deserializerFactory
                .getKeyDeserializer("consumerFactory"))
                .isInstanceOf(StringDeserializer::class.java)

    }

    @Test
    fun `ensure deserializers retrieved from the factory are prototypes`() {
        assertThat(this.consumerFactory.deserializerFactory.getKeyDeserializer("consumerFactory"))
                .isNotSameAs(this.consumerFactory.deserializerFactory.getKeyDeserializer("consumerFactory"))
    }

    @Configuration
    @EnableKafka
    class Config {

        @Bean
        fun consumerFactory(): FactorySuppliedDeserializerKafkaConsumerFactory<String, String>
                = FactorySuppliedDeserializerKafkaConsumerFactory(HashMap<String, Any>())

        @Bean
        fun deserializerFactory(): KafkaDeserializerFactory<String, String> = BeanLookupKafkaDeserializerFactory<String, String>()

        @Bean
        fun consumerFactoryWithProvidedDeserializerFactory(): FactorySuppliedDeserializerKafkaConsumerFactory<String, String> {
            val factory: FactorySuppliedDeserializerKafkaConsumerFactory<String, String>
                    = FactorySuppliedDeserializerKafkaConsumerFactory(HashMap<String, Any>())
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
        @KafkaKeyDeserializer(consumerFactories = ["consumerFactory"])
        fun configurationDeserializer(): Deserializer<String> = StringDeserializer()

    }
//TODO how to component scan this
    @Component
    @KafkaKeyDeserializer
    class ComponentDeserializer: IntegerDeserializer()
}