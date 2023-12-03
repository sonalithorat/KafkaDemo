package com.kafkademo.kafkademo.producer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkademo.kafkademo.domain.LibraryEvent;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventProducer {
	private KafkaTemplate<Integer, String> kafkaTemplate;

	private final ObjectMapper objectMapper;

	@Value("${spring.kafka.topic}")
	public String topic;

	public LibraryEventProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
	}

	public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent libraryEvent)
			throws JsonProcessingException {
		var key = libraryEvent.libraryEventId();
		var value = objectMapper.writeValueAsString(libraryEvent);

		// 1 blocking call- get metadata about the kafka cluster
		// 2. send messgae happens - return completable feature.
		var completableFture = kafkaTemplate.send(topic, key, value);

		return completableFture.whenComplete((sendResult, throwable) -> {
			if (throwable != null) {
				handleError(key, value, throwable);
			} else {
				handleSuccess(key, value, sendResult);

			}
		});
	}

	public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent_ProducerApproach(LibraryEvent libraryEvent)
			throws JsonProcessingException {
		var key = libraryEvent.libraryEventId();
		var value = objectMapper.writeValueAsString(libraryEvent);

		var producerRecord = buildProducerRecord(key, value);
		// 1 blocking call- get metadata about the kafka cluster
		// 2. send messgae happens - return completable feature.
		var completableFture = kafkaTemplate.send(producerRecord);

		return completableFture.whenComplete((sendResult, throwable) -> {
			if (throwable != null) {
				handleError(key, value, throwable);
			} else {
				handleSuccess(key, value, sendResult);

			}
		});
	}
	
	private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {
		// TODO Auto-generated method stub
		return new ProducerRecord<Integer, String>(topic, key, value);
	}

	public SendResult<Integer, String> sendLibraryEventSync(LibraryEvent libraryEvent)
			throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException {
		var key = libraryEvent.libraryEventId();
		var value = objectMapper.writeValueAsString(libraryEvent);

		// 1 blocking call- get metadata about the kafka cluster
		// 2. block and wait until the message is send to the kafka.
		var sendresult = kafkaTemplate.send(topic, key, value).get(3, TimeUnit.SECONDS);
		handleSuccess(key, value, sendresult);
		return sendresult;
	}

	private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
		log.info("message sent successfully for the key: {} and value: {}, partition is :{}", key, value,
				sendResult.getRecordMetadata().partition());
	}

	private void handleError(Integer key, String value, Throwable ex) {
		log.error("Error sending the message and exception is {}", ex.getMessage(), ex);

	}

}
