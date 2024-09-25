package me.jeremiah.databases.nosql;

import me.jeremiah.Entry;
import me.jeremiah.databases.Database;
import org.jetbrains.annotations.NotNull;
import org.neo4j.driver.Record;
import org.neo4j.driver.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4j implements Database {

  private Driver driver;

  @Override
  public String getName() {
    return "Neo4j";
  }

  @Override
  public void open() {
    Config config = Config.builder()
      .withMaxConnectionPoolSize(50) // Connection Pooling
      .build();
    driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.none(), config);
    try (Session session = driver.session(SessionConfig.forDatabase("neo4j"))) { // Session Configuration
      session.run("CREATE INDEX IF NOT EXISTS FOR (e:Entry) ON (e.id)");
    }
  }

  @Override
  public void close() {
    if (driver != null) {
      driver.close();
    }
  }

  @Override
  public void wipe() {
    try (Session session = driver.session(SessionConfig.forDatabase("neo4j"))) {
      session.run("MATCH (n) DETACH DELETE n");
    }
  }

  @Override
  public void insert(@NotNull Entry @NotNull ... entries) {
    try (Session session = driver.session(SessionConfig.forDatabase("neo4j"));
         Transaction tx = session.beginTransaction()) {
      List<Value> records = new ArrayList<>();
      for (Entry entry : entries) {
        byte[] entryBytes = entry.bytes();
        records.add(Values.parameters("id", entry.getId(), "data", entryBytes));
      }
      tx.run("UNWIND $batch AS row CREATE (e:Entry {id: row.id, data: row.data})", Values.parameters("batch", records));
      tx.commit();
    }
  }

  @Override
  public void update(@NotNull Entry @NotNull ... entries) {
    try (Session session = driver.session(SessionConfig.forDatabase("neo4j"));
         Transaction tx = session.beginTransaction()) {
      List<Value> records = new ArrayList<>();
      for (Entry entry : entries) {
        byte[] entryBytes = entry.bytes();
        records.add(Values.parameters("id", entry.getId(), "data", entryBytes));
      }
      tx.run("UNWIND $batch AS row MATCH (e:Entry {id: row.id}) SET e.data = row.data", Values.parameters("batch", records));
      tx.commit();
    }
  }

  @Override
  public boolean exists(int id) {
    try (Session session = driver.session(SessionConfig.forDatabase("neo4j"))) {
      Result result = session.run("MATCH (e:Entry {id: $id}) RETURN e", Values.parameters("id", id));
      return result.hasNext();
    }
  }

  @Override
  public void remove(@NotNull Integer @NotNull ... ids) {
    try (Session session = driver.session(SessionConfig.forDatabase("neo4j"));
         Transaction tx = session.beginTransaction()) {
      List<Integer> idList = new ArrayList<>();
      for (int id : ids) {
        idList.add(id);
      }
      tx.run("UNWIND $ids AS id MATCH (e:Entry {id: id}) DETACH DELETE e", Values.parameters("ids", idList));
      tx.commit();
    }
  }

  @Override
  public Map<Integer, Entry> select() {
    Map<Integer, Entry> entries = new HashMap<>();
    try (Session session = driver.session(SessionConfig.forDatabase("neo4j"))) {
      Result result = session.run("MATCH (e:Entry) RETURN e.id AS id, e.data AS data");
      while (result.hasNext()) {
        Record record = result.next();
        int id = record.get("id").asInt();
        byte[] data = record.get("data").asByteArray();
        Entry entry = new Entry(id, data);
        entries.put(id, entry);
      }
    }
    return entries;
  }
}