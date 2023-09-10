/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.github.thmarx.mongo.trigger;

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

import com.mongodb.client.MongoDatabase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author t.marx
 */
public class MongoTriggers implements AutoCloseable {

	ChangeStreamWatcher updater;
	
	MultiMap<Event, DocumentTrigger> documentTrigger;
	List<DatabaseTrigger> databaseTrigger;
	List<CollectionTrigger> collectionTrigger;

	final Configuration configuration;

	public MongoTriggers () {
		this(new Configuration());
	}

	public MongoTriggers(Configuration configuration) {
		this.configuration = configuration;
		this.documentTrigger = new MultiMap<>();
		databaseTrigger = new ArrayList<>();
		collectionTrigger = new ArrayList<>();
	}
	
	public void register(final Event event, final DocumentTrigger trigger) {
		documentTrigger.put(event, trigger);
	}
	
	public void register(final CollectionTrigger trigger) {
		collectionTrigger.add(trigger);
	}
	
	public void register(final DatabaseTrigger trigger) {
		databaseTrigger.add(trigger);
	}
	

	@Override
	public void close() throws Exception {
		updater.close();
	}

	public void open(MongoDatabase database) throws IOException {
		
		updater = new ChangeStreamWatcher(database, this.configuration, documentTrigger, databaseTrigger, collectionTrigger);
		
		updater.connect();
	}
}
