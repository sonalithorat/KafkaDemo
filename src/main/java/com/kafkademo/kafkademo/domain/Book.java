package com.kafkademo.kafkademo.domain;

public record Book(
		 Integer bookId,
		 String bookName,
		 String bookAuthor
		) {

}
