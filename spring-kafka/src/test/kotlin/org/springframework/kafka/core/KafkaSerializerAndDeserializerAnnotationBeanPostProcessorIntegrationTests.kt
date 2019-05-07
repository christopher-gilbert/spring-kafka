package org.springframework.kafka.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig
import javax.annotation.Resource

@SpringJUnitConfig
@DirtiesContext
class KafkaSerializerAndDeserializerAnnotationBeanPostProcessorIntegrationTests {

    @Resource(name = "consumerFactoryWithoutDeserializerFactory")
    private lateinit var consumerFactoryWithoutDeserializerFactory: FactorySuppliedDeserializerKafkaConsumerFactory<String, String>

    @Resource(name = "consumerFactoryWithDeserializerFactory")
    private lateinit var consumerFactoryWithDeserializerFactory: FactorySuppliedDeserializerKafkaConsumerFactory<String, String>

    @Autowired
    private lateinit var existingDeserializerFactory: KafkaDeserializerFactory<String, String>

    @Test
    fun `ensure new deserializer factory is injected into consumer factory`() {
        assertThat(this.consumerFactoryWithoutDeserializerFactory.hasDeserializerFactory()).isTrue()
        assertThat(this.consumerFactoryWithoutDeserializerFactory.deserializerFactory)
                .isInstanceOf(BeanLookupKafkaDeserializerFactory::class.java)
    }

    @Test
    fun `ensure new existing deserializer factory is not replaced`() {
        assertThat(this.consumerFactoryWithDeserializerFactory.hasDeserializerFactory()).isTrue()
        assertThat(this.consumerFactoryWithDeserializerFactory.deserializerFactory).isSameAs(existingDeserializerFactory)
    }

    @Configuration
    @EnableKafka
    class Config {

        @Bean
        fun consumerFactoryWithoutDeserializerFactory(): FactorySuppliedDeserializerKafkaConsumerFactory<String, String>
                = FactorySuppliedDeserializerKafkaConsumerFactory(HashMap<String, Any>())

        @Bean
        fun deserializerFactory(): KafkaDeserializerFactory<String, String> = BeanLookupKafkaDeserializerFactory<String, String>()

        @Bean
        fun consumerFactoryWithDeserializerFactory(): FactorySuppliedDeserializerKafkaConsumerFactory<String, String> {
            val factory: FactorySuppliedDeserializerKafkaConsumerFactory<String, String>
                    = FactorySuppliedDeserializerKafkaConsumerFactory(HashMap<String, Any>())
            factory.deserializerFactory = deserializerFactory()
            return factory
        }

        @Bean
        fun kafkaListenerContainerFactory1(): ConcurrentKafkaListenerContainerFactory<String, String> {
            val factory: ConcurrentKafkaListenerContainerFactory<String, String> = ConcurrentKafkaListenerContainerFactory()
            factory.consumerFactory = consumerFactoryWithoutDeserializerFactory()
            return factory
        }

        @Bean
        fun kafkaListenerContainerFactory2(): ConcurrentKafkaListenerContainerFactory<String, String> {
            val factory: ConcurrentKafkaListenerContainerFactory<String, String> = ConcurrentKafkaListenerContainerFactory()
            factory.consumerFactory = consumerFactoryWithDeserializerFactory()
            return factory
        }

    }
}