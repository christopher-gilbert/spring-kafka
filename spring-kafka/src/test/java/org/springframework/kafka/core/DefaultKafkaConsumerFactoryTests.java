package org.springframework.kafka.core;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultKafkaConsumerFactoryTests {

	@Test
	public void testProvidedDeserializersAreShared() {
		ConsumerFactory<String, String> target = new DefaultKafkaConsumerFactory<>(Collections.emptyMap(), new StringDeserializer() {

		}, null);
		assertThat(target.getKeyDeserializer()).isSameAs(target.getKeyDeserializer());
	}
}


