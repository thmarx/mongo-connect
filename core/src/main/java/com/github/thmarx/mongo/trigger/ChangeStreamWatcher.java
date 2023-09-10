/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.github.thmarx.mongo.trigger;

import java.util.List;

import org.bson.Document;

/*-
 * #%L
 * mongo-trigger-index
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
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author t.marx
 */
@RequiredArgsConstructor
@Slf4j
public class ChangeStreamWatcher implements AutoCloseable {

	private final MongoDatabase database;
	private final Configuration configuration;

	private final MultiMap<Event, DocumentTrigger> documentTrigger;
	private final List<DatabaseTrigger> databaseTrigger;
	private final List<CollectionTrigger> collectionTrigger;

	private Thread watcher;

	private boolean closed = false;

	public void connect() {
		
		watcher = new Thread(() -> {
			int retryCounter = 0;
			while (!closed) {
				try {
					log.debug("try to connect to " + database.getName() + " changestream");
					ChangeStreamIterable<Document> watch = database.watch().fullDocument(FullDocument.UPDATE_LOOKUP);
					retryCounter = 0;
					log.debug("connection to " + database.getName() + " established");
					watch.forEach(this::handle);
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
		if (isCollectionRelevant(document)) {
			var collection = document.getNamespace().getCollectionName();
			var databaseName = document.getNamespace().getDatabaseName();
			switch (document.getOperationType()) {
				case DELETE -> documentTrigger.get(Event.DELETE)
						.forEach((function) -> function.accept(databaseName, collection, document));
				case INSERT -> documentTrigger.get(Event.INSERT)
						.forEach((function) -> function.accept(databaseName, collection, document));
				case UPDATE -> documentTrigger.get(Event.UPDATE)
						.forEach((function) -> function.accept(databaseName, collection, document));
				case DROP -> collectionTrigger
						.forEach(trigger -> trigger.accept(CollectionTrigger.Type.DROPPED, databaseName, collection));
				case DROP_DATABASE ->
					databaseTrigger.forEach(trigger -> trigger.accept(DatabaseTrigger.Type.DROPPED, databaseName));
			}
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
