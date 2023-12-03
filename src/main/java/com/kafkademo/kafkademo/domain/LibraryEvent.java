package com.kafkademo.kafkademo.domain;

import com.kafkademo.kafkademo.Enum.LibraryEventType;

public record LibraryEvent(
		
			Integer libraryEventId,
			LibraryEventType libraryEventType,
			Book book
		) {

}
