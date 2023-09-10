package com.github.thmarx.mongo.trigger;

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
