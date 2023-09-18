
# mongo-connect

Mongo-connect

## Examples

### create instance

```java

MongoDatabase database = mongoClient.getDatabase("a-database");

MongoConnect mongoConnect = new MongoConnect();
mongoConnect.open(database);
```