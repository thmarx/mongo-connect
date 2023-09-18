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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.bson.Document;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author t.marx
 */
public class MongoConnectNGTest extends AbstractContainerTest {

	private MongoDatabase database;

	MongoConnect mongoConnect;
	
	@BeforeMethod
	public void setup() throws IOException {

		database = mongoClient.getDatabase("search");

		if (database.getCollection("trigger") != null) {
			database.getCollection("trigger").drop();
		}
		//database.createCollection("trigger");

		mongoConnect = new MongoConnect();
		mongoConnect.open(database);
	}

	@Test
	public void test_connect() throws InterruptedException {
		
		final AtomicInteger counter = new AtomicInteger(0);
		
		mongoConnect.register(Event.INSERT, (database, collection, document) -> {
			counter.incrementAndGet();
		});
		
		insertDocument("trigger", Map.of("name", "thorsten"));
		
		Awaitility.await().atMost(10, TimeUnit.MINUTES).until(() -> counter.get() > 0);
		
		Assertions.assertThat(counter).hasValue(1);
	}
	
	private void insertDocument(final String collectionName, final Map attributes) {
		MongoCollection<Document> collection = database.getCollection(collectionName);
		Document document = new Document(attributes);
		collection.insertOne(document);
	}

}
