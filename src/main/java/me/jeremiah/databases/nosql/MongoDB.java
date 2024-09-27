package me.jeremiah.databases.nosql;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import me.jeremiah.Entry;
import me.jeremiah.databases.Database;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class MongoDB implements Database {

  protected final MongoClientSettings settings = MongoClientSettings.builder()
    .applyConnectionString(new ConnectionString("mongodb://localhost:27017/data"))
    .uuidRepresentation(UuidRepresentation.STANDARD)
    .applyToConnectionPoolSettings(builder -> builder.maxSize(50))
    .build();
  private MongoClient client;
  private MongoCollection<Document> entries;

  @Override
  public void open() {
    if (client != null)
      throw new IllegalStateException("Client is already open");
    client = MongoClients.create(settings);
    MongoDatabase database = client.getDatabase("data");
    entries = database.getCollection("entries");
    entries.createIndex(new Document("id", 1), new IndexOptions().unique(true));
  }

  @Override
  public void wipe() {
    entries.deleteMany(new Document());
  }

  @Override
  public void close() {
    if (client == null)
      throw new IllegalStateException("Client is already closed");
    client.close();
    client = null;
  }

  @Override
  public void insert(@NotNull Entry @NotNull ... entries) {
    bulkWrite(entries, writable -> new InsertOneModel<>(writable.toDocument()));
  }

  @Override
  public void update(@NotNull Entry @NotNull ... entries) {
    bulkWrite(entries, writable -> new UpdateOneModel<>(new Document("id", writable.getId()), new Document("$set", writable.toDocument())));
  }

  @Override
  public void remove(@NotNull Integer @NotNull ... ids) {
    bulkWrite(ids, writable -> new DeleteOneModel<>(new Document("id", writable)));
  }
  
  private <W> void bulkWrite(W[] writables, Function<W, WriteModel<Document>> converter) {
    List<WriteModel<Document>> updates = Arrays.stream(writables).map(converter).toList();
    this.entries.bulkWrite(updates, new BulkWriteOptions().ordered(false));
  }

  public boolean exists(int id) {
    return entries.find(new Document("id", id)).first() != null;
  }

  @Override
  public Map<Integer, Entry> select() {
    Map<Integer, Entry> entries = new HashMap<>();
    for (Document document : this.entries.find()) {
      int id = document.getInteger("id");
      entries.put(id, new Entry(id,
        document.getString("first_name"),
        document.getString("middle_initial").charAt(0),
        document.getString("last_name"),
        document.getInteger("age"),
        document.getDouble("net_worth")
      ));
    }
    return entries;
  }

}
