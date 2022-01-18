package com.github.timtebeek.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

@Configuration
@EnableKafka
public class KafkaConfiguration implements KafkaListenerConfigurer {

	public static final String ORDERS = "orders";

	@Bean
	public NewTopic ordersTopic() {
		return TopicBuilder.name(ORDERS)
				// Use more than one partition for frequently used input topic
				.partitions(6)
				.build();
	}

	@Bean
	public NewTopic deadLetterTopic(AppKafkaProperties properties) {
		// https://docs.spring.io/spring-kafka/docs/2.8.1/reference/html/#configuring-topics
		return TopicBuilder.name(ORDERS + properties.deadletter().suffix())
				// Use only one partition for infrequently used Dead Letter Topic
				.partitions(1)
				// Use longer retention for Dead Letter Topic, allowing for more time to troubleshoot
				.config(TopicConfig.RETENTION_MS_CONFIG, "" + properties.deadletter().retention().toMillis())
				.build();
	}

	@Bean
	public DefaultErrorHandler defaultErrorHandler(
			KafkaOperations<Object, Object> operations,
			AppKafkaProperties properties) {
		// Publish to dead letter topic any messages dropped after retries with back off
		var recoverer = new DeadLetterPublishingRecoverer(operations,
				// Always send to first/only partition of DLT suffixed topic
				(cr, e) -> new TopicPartition(cr.topic() + properties.deadletter().suffix(), 0));

		// Spread out attempts over time, taking a little longer between each attempt
		// Set a max for retries below max.poll.interval.ms; default: 5m, as otherwise we trigger a consumer rebalance
		Backoff backoff = properties.backoff();
		var exponentialBackOff = new ExponentialBackOffWithMaxRetries(backoff.maxRetries());
		exponentialBackOff.setInitialInterval(backoff.initialInterval().toMillis());
		exponentialBackOff.setMultiplier(backoff.multiplier());
		exponentialBackOff.setMaxInterval(backoff.maxInterval().toMillis());

		// Do not try to recover from validation exceptions when validation of orders failed
		var errorHandler = new DefaultErrorHandler(recoverer, exponentialBackOff);
		errorHandler.addNotRetryableExceptions(javax.validation.ValidationException.class);

		return errorHandler;
	}

	@Autowired
	private LocalValidatorFactoryBean validator;

	@Override
	public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
		// https://docs.spring.io/spring-kafka/docs/2.8.1/reference/html/#kafka-validation
		registrar.setValidator(this.validator);
	}
}
