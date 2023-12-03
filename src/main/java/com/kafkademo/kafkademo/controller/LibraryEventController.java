package com.kafkademo.kafkademo.controller;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkademo.kafkademo.domain.LibraryEvent;
import com.kafkademo.kafkademo.producer.LibraryEventProducer;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class LibraryEventController {
	
	private final LibraryEventProducer libraryEventProducer;
	
	public LibraryEventController(LibraryEventProducer libraryEventProducer) {
		super();
		this.libraryEventProducer = libraryEventProducer;
	}

	

	@RequestMapping(value="/create", method = RequestMethod.POST)
	public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent)
			throws JsonProcessingException{
		log.info("logger");
		libraryEventProducer.sendLibraryEvent(libraryEvent);
		return ResponseEntity.ok().body(libraryEvent);
		
	}
	
	@RequestMapping(value="/creates", method = RequestMethod.POST)
	public ResponseEntity<LibraryEvent> postLibraryEvent_prodApproach(@RequestBody LibraryEvent libraryEvent)
			throws JsonProcessingException{
		log.info("logger");
		libraryEventProducer.sendLibraryEvent(libraryEvent);
		return ResponseEntity.ok().body(libraryEvent);
		
	}
	
	//synchronous call example
	@RequestMapping(value="/produce", method = RequestMethod.POST)
	public ResponseEntity<LibraryEvent> postLibraryEventSync(@RequestBody LibraryEvent libraryEvent)
			throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException{
		log.info("logger");
		libraryEventProducer.sendLibraryEventSync(libraryEvent);
		return ResponseEntity.ok().body(libraryEvent);
		
	}
}
