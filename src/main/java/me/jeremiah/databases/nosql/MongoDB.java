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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoDB implements Database {

  protected final MongoClientSettings settings;
  private MongoClient client;
  private MongoDatabase data;
  private MongoCollection<Document> entries;

  public MongoDB() {
    settings = MongoClientSettings.builder()
      .applyConnectionString(new ConnectionString("mongodb://localhost:27017/data"))
      .uuidRepresentation(UuidRepresentation.STANDARD)
      .applyToConnectionPoolSettings(builder -> builder.maxSize(50))
      .build();
  }

  @Override
  public String getName() {
    return "MongoDB";
  }

  @Override
  public void open() {
    if (client != null)
      throw new IllegalStateException("Client is already open");
    client = MongoClients.create(settings);
    data = client.getDatabase("data");
    entries = data.getCollection("entries");
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
  public void insert(Entry... entries) {
    List<Document> documents = new ArrayList<>();
    for (Entry entry : entries)
      documents.add(entry.toDocument());
    this.entries.insertMany(documents);
  }

  @Override
  public void update(Entry... entries) {
    List<WriteModel<Document>> updates = new ArrayList<>();
    for (Entry entry : entries)
      updates.add(new UpdateOneModel<>(new Document("id", entry.getId()), new Document("$set", entry.toDocument())));
    this.entries.bulkWrite(updates, new BulkWriteOptions().ordered(false));
  }

  public boolean exists(int id) {
    return entries.find(new Document("id", id)).first() != null;
  }

  @Override
  public void remove(int... ids) {
    List<WriteModel<Document>> deletes = new ArrayList<>();
    for (int id : ids)
      deletes.add(new DeleteOneModel<>(new Document("id", id)));
    this.entries.bulkWrite(deletes, new BulkWriteOptions().ordered(false));
  }

  @Override
  public Map<Integer, Entry> select() {
    Map<Integer, Entry> entries = new HashMap<>();
    for (Document document : this.entries.find()) {
      int id = document.getInteger("id");
      entries.put(id, new Entry(id,
        document.getString("first_name"),
        document.getString("middle_initial").charAt(0),
        document.getString("last_name")
      ));
    }
    return entries;
  }

}
