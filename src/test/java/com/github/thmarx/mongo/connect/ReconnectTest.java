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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 *
 * @author t.marx
 */
public class ReconnectTest extends AbstractContainerTest {

	private MongoDatabase database;

	MongoConnect mongoTriggers;
	
	@BeforeMethod
	public void setup() throws IOException {

		database = mongoClient.getDatabase("search");

		if (database.getCollection("trigger") != null) {
			database.getCollection("trigger").drop();
		}
		//database.createCollection("trigger");

		mongoTriggers = new MongoConnect(new Configuration()
			.connectRetryDelay(TimeUnit.SECONDS.toMillis(1))
		);
		mongoTriggers.open(database);
	}

	@Test
	public void test_reconnect() throws InterruptedException {
		
		final AtomicInteger counter = new AtomicInteger(0);
		
		mongoTriggers.register(Event.INSERT, (database, collection, document) -> {
			counter.incrementAndGet();
		});
		
		insertDocument(database, "trigger", Map.of("name", "thorsten"));
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> counter.get() == 1);
		Assertions.assertThat(counter).hasValue(1);

		System.out.println("disconnect");
		mongdbContainer.getDockerClient().pauseContainerCmd(mongdbContainer.getContainerId()).exec();

		System.out.println("reconnect");
		mongdbContainer.getDockerClient().unpauseContainerCmd(mongdbContainer.getContainerId()).exec();

		insertDocument(database, "trigger", Map.of("name", "thorsten"));
		Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> counter.get() == 2);
		Assertions.assertThat(counter).hasValue(2);
	}
	
	

}
