package com.github.timtebeek;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest
@Testcontainers
class KafkaDeadLetterPublishingApplicationTests {

	private static final String ORDERS_DLT = "orders.DLT";

	private static final Logger log = LoggerFactory.getLogger(KafkaDeadLetterPublishingApplicationTests.class);

	@Container // https://www.testcontainers.org/modules/kafka/
	static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.1"));

	@DynamicPropertySource
	static void setProperties(DynamicPropertyRegistry registry) {
		// Connect our Spring application to our Testcontainers Kafka instance
		registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
	}

	@Autowired
	private KafkaOperations<String, Order> operations;

	private static KafkaConsumer<String, String> kafkaConsumer;

	@BeforeAll
	static void setup() {
		// Create a test consumer that handles <String, String> records, listening to orders.DLT
		// https://docs.spring.io/spring-kafka/docs/current/reference/html/#testing
		var consumerProps = KafkaTestUtils.consumerProps(kafka.getBootstrapServers(), "test-consumer", "true");
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaConsumer = new KafkaConsumer<>(consumerProps);
		kafkaConsumer.subscribe(List.of(ORDERS_DLT));
	}

	@AfterAll
	static void close() {
		// Close the consumer before shutting down Testcontainers Kafka instance
		kafkaConsumer.close();
	}

	@Test
	void should_not_produce_onto_dlt_for_ok_message() {
		// Send in valid order
		Order order = new Order(randomUUID(), randomUUID(), 1);
		operations.send("orders", order.orderId().toString(), order)
				.whenCompleteAsync((result, ex) -> {
					if (ex == null) {
						log.info("Success: {}", result);
					}
					else {
						log.error("Failure ", ex);
					}
				});

		// Verify no message was produced onto Dead Letter Topic
		assertThrows(
				IllegalStateException.class,
				() -> KafkaTestUtils.getSingleRecord(kafkaConsumer, ORDERS_DLT, Duration.ofSeconds(5)),
				"No records found for topic");
	}

	@Test
	void should_produce_onto_dlt_for_bad_message() {
		// Amount can not be negative, validation will fail
		Order order = new Order(randomUUID(), randomUUID(), -2);
		operations.send("orders", order.orderId().toString(), order)
				.whenCompleteAsync((result, ex) -> {
					if (ex == null) {
						log.info("Success: {}", result);
					}
					else {
						log.error("Failure ", ex);
					}
				});

		// Verify message produced onto Dead Letter Topic
		ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(kafkaConsumer, ORDERS_DLT, Duration.ofSeconds(2));

		// Verify headers present, and single header value
		Headers headers = record.headers();
		assertThat(headers).map(Header::key).containsAll(List.of(
				"kafka_dlt-exception-fqcn",
				"kafka_dlt-exception-cause-fqcn",
				"kafka_dlt-exception-message",
				"kafka_dlt-exception-stacktrace",
				"kafka_dlt-original-topic",
				"kafka_dlt-original-partition",
				"kafka_dlt-original-offset",
				"kafka_dlt-original-timestamp",
				"kafka_dlt-original-timestamp-type",
				"kafka_dlt-original-consumer-group"));
		assertThat(new String(headers.lastHeader("kafka_dlt-exception-fqcn").value()))
				.isEqualTo("org.springframework.kafka.listener.ListenerExecutionFailedException");
		assertThat(new String(headers.lastHeader("kafka_dlt-exception-cause-fqcn").value()))
				.isEqualTo("org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException");
		assertThat(new String(headers.lastHeader("kafka_dlt-exception-message").value()))
				.contains("Listener method could not be invoked with the incoming message");

		// Verify payload value matches sent in order
		assertThat(record.value()).isEqualToIgnoringWhitespace("""
				{ "orderId": "%s", "articleId": "%s", "amount":-2 }""".formatted(order.orderId(), order.articleId()));
	}

}