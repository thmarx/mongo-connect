
# mongo-connect

mongo-connect supports using the MongoDB changestream.

## Examples

### create instance

```java

MongoDatabase database = mongoClient.getDatabase("test-database");

MongoConnect mongoConnect = new MongoConnect();
// watch global for all changes
mongoConnect.open(mongoConnect.open(() -> monogClient.watch());
// watch only for database changes
mongoConnect.open(mongoConnect.open(() -> database.watch());
// watch only for collection changes
mongoConnect.open(mongoConnect.open(() -> database.getCollection("test-collection").watch());
```

### listen for database events

```java
mongoConnect.register(Event.DROP, (name) -> {
	System.out.println("database %s dropped".formatted(name));
});
```

### listen for collection events

```java
mongoConnect.register(Event.DROP, (databaseName, collectionName) -> {
	System.out.println("collection %s of database %s dropped".formatted(databaseName, collectionName));
});
```

### listen for document events

```java
mongoConnect.register(Event.INSERT, (databaseName, collectionName, document) -> {
	System.out.println("new document inserted into collection %s".format(collectionName));
});
```