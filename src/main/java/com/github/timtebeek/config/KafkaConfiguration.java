package com.github.timtebeek.config;

import org.apache.kafka.clients.admin.NewTopic;
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
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

@Configuration
@EnableKafka
public class KafkaConfiguration implements KafkaListenerConfigurer {

	public static final String ORDERS = "orders";

	@Bean
	public NewTopic ordersTopic() {
		return TopicBuilder.name(ORDERS).build();
	}

	@Bean
	public NewTopic deadLetterTopic(AppKafkaProperties properties) {
		// https://docs.spring.io/spring-kafka/reference/html/#configuring-topics
		return TopicBuilder.name(ORDERS + properties.deadletter().suffix())
				.config(TopicConfig.RETENTION_MS_CONFIG, "" + properties.deadletter().retention().toMillis())
				.build();
	}

	@Bean
	public DefaultErrorHandler seekToCurrentErrorHandler(
			KafkaOperations<Object, Object> operations,
			AppKafkaProperties properties) {
		// Publish to dead letter topic any messages dropped after retries with back off
		var recoverer = new DeadLetterPublishingRecoverer(operations);

		// Spread out attempts over time, taking a little longer between each attempt
		Backoff backoffProperties = properties.backoff();
		var backOff = new ExponentialBackOff(backoffProperties.initialInterval().toMillis(),
				backoffProperties.multiplier());
		// Set a max for retries below max.poll.interval.ms; default: 5m, as otherwise we trigger consumer rebalance
		backOff.setMaxElapsedTime(backoffProperties.maxElapsedTime().toMillis());

		// Do not try to recover from validation exceptions when validation of orders failed
		var seekToCurrentErrorHandler = new DefaultErrorHandler(recoverer, backOff);
		seekToCurrentErrorHandler.addNotRetryableExceptions(
				javax.validation.ValidationException.class);

		return seekToCurrentErrorHandler;
	}

	@Autowired
	private LocalValidatorFactoryBean validator;

	@Override
	public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
		registrar.setValidator(this.validator);
	}
}
