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

import java.util.Map;

import org.bson.Document;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 *
 * @author t.marx
 */
public class AbstractContainerTest {

	protected MongoDBContainer mongdbContainer;
	protected MongoClient mongoClient;

	protected Network network;

	@BeforeClass
	public void up() {
		network = Network.newNetwork();
		mongdbContainer = new MongoDBContainer(DockerImageName.parse(
				"mongo:6.0.9"
		)).withNetwork(network);
		mongdbContainer.start();

        System.out.println("connection " + mongdbContainer.getConnectionString());
		mongoClient = MongoClients.create(mongdbContainer.getConnectionString());
	}

	@AfterClass
	public void down() {
		mongoClient.close();
		mongdbContainer.stop();
		network.close();
	}

    protected void insertDocument(final MongoDatabase database, final String collectionName, final Map<String,?> attributes) {
		MongoCollection<Document> collection = database.getCollection(collectionName);
		Document document = new Document(attributes);
		collection.insertOne(document);
	}
}
