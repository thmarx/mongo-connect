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

import com.mongodb.client.ChangeStreamIterable;
import java.io.IOException;
import java.util.function.Supplier;
import org.bson.Document;

/**
 *
 * @author t.marx
 */
public class MongoConnect implements AutoCloseable {

	ChangeStreamWatcher updater;
	
	MultiMap<Event, DocumentFunction> documentFunctions;
	MultiMap<Event, DatabaseFunction> databaseFunctions;
	MultiMap<Event, CollectionFunction> collectionFunctions;

	final Configuration configuration;

	public MongoConnect () {
		this(new Configuration());
	}

	public MongoConnect(Configuration configuration) {
		this.configuration = configuration;
		this.documentFunctions = new MultiMap<>();
		databaseFunctions = new MultiMap<>();
		collectionFunctions = new MultiMap<>();
	}
	
	public void register(final Event event, final DocumentFunction function) {
		documentFunctions.put(event, function);
	}
	
	public void register(final Event event, final CollectionFunction function) {
		collectionFunctions.put(event, function);
	}
	
	public void register(final Event event, final DatabaseFunction function) {
		databaseFunctions.put(event, function);
	}
	

	@Override
	public void close() throws Exception {
		if (updater != null) {
			updater.close();
			updater = null;
		}
	}

	public void open(Supplier<ChangeStreamIterable<Document>> changeStreamSupplier) throws IOException {
		if (updater != null) {
			throw new RuntimeException("already open");
		}
		updater = new ChangeStreamWatcher(changeStreamSupplier, this.configuration, documentFunctions, databaseFunctions, collectionFunctions);
		updater.connect();
	}
}
