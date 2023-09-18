package com.github.thmarx.mongo.connect;

import java.util.List;

import org.bson.Document;

/*-
 * #%L
 * mongo-connect
 * %%
 * Copyright (C) 2023 Marx-Software
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import java.util.function.Supplier;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author t.marx
 */
@RequiredArgsConstructor
@Slf4j
public class ChangeStreamWatcher implements AutoCloseable {

	private final Supplier<ChangeStreamIterable<Document>> changeStreamSupplier;
	private final Configuration configuration;

	private final MultiMap<Event, DocumentFunction> documentFunctions;
	private final MultiMap<Event, DatabaseFunction> databaseFunctions;
	private final MultiMap<Event, CollectionFunction> collectionFunctions;

	private Thread watcher;

	private boolean closed = false;

	public void connect() {

		watcher = new Thread(() -> {
			int retryCounter = 0;
			while (!closed) {
				try {
					log.debug("try to connect to client changestream");
					ChangeStreamIterable<Document> watch = changeStreamSupplier.get().fullDocument(FullDocument.UPDATE_LOOKUP);
					retryCounter = 0;
					log.debug("connection to client established");
					watch.forEach(this::handle);
					log.debug("connection to client lost");
				} catch (Exception e) {
					if (!closed && !watcher.isInterrupted() && retryCounter < configuration.connectRetries) {
						try {
							log.debug("connection lost, reconnect after " + configuration.connectRetryDelay());
							Thread.sleep(configuration.connectRetryDelay());
							retryCounter++;
						} catch (InterruptedException e1) {
							// interrupted nothing to do
							closed = true;
							log.debug("change stream monitor closed");
						}
					} else {
						log.debug("change stream monitor closed");
					}
				}
			}
		}, "ChangeStreamUpdated");

		watcher.start();
	}

	public void handle(ChangeStreamDocument<Document> document) {
		var collection = document.getNamespace().getCollectionName();
		var databaseName = document.getNamespace().getDatabaseName();
		switch (document.getOperationType()) {
			case DELETE ->
				documentFunctions.get(Event.DELETE)
						.forEach((function) -> function.accept(databaseName, collection, document));
			case INSERT ->
				documentFunctions.get(Event.INSERT)
						.forEach((function) -> function.accept(databaseName, collection, document));
			case UPDATE ->
				documentFunctions.get(Event.UPDATE)
						.forEach((function) -> function.accept(databaseName, collection, document));
			case DROP ->
				collectionFunctions.get(Event.DROP)
						.forEach(function -> function.accept(databaseName, collection));
			case DROP_DATABASE ->
				databaseFunctions.get(Event.DROP)
						.forEach(function -> function.accept(databaseName));
		}

	}

	private boolean isCollectionRelevant(ChangeStreamDocument<Document> document) {
		return true;
	}

	@Override
	public void close() {
		closed = true;
		watcher.interrupt();
	}
}
