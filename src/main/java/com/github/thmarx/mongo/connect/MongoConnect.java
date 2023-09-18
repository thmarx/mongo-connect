package com.github.thmarx.mongo.connect;

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

import com.mongodb.client.MongoDatabase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author t.marx
 */
public class MongoConnect implements AutoCloseable {

	ChangeStreamWatcher updater;
	
	MultiMap<Event, DocumentFunction> documentFunctions;
	List<DatabaseFunction> databaseFunctions;
	List<CollectionFunction> collectionFunctions;

	final Configuration configuration;

	public MongoConnect () {
		this(new Configuration());
	}

	public MongoConnect(Configuration configuration) {
		this.configuration = configuration;
		this.documentFunctions = new MultiMap<>();
		databaseFunctions = new ArrayList<>();
		collectionFunctions = new ArrayList<>();
	}
	
	public void register(final Event event, final DocumentFunction trigger) {
		documentFunctions.put(event, trigger);
	}
	
	public void register(final CollectionFunction trigger) {
		collectionFunctions.add(trigger);
	}
	
	public void register(final DatabaseFunction trigger) {
		databaseFunctions.add(trigger);
	}
	

	@Override
	public void close() throws Exception {
		updater.close();
	}

	public void open(MongoDatabase database) throws IOException {
		
		updater = new ChangeStreamWatcher(database, this.configuration, documentFunctions, databaseFunctions, collectionFunctions);
		
		updater.connect();
	}
}
