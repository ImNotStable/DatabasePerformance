package me.jeremiah.databases.nosql;

import me.jeremiah.Entry;
import me.jeremiah.databases.Database;
import org.jetbrains.annotations.NotNull;
import org.neo4j.driver.Record;
import org.neo4j.driver.*;

import java.util.*;

public class Neo4j implements Database {

  private Driver driver;

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
    alterEntries("UNWIND $batch AS row CREATE (e:Entry {id: row.id, first_name: row.first_name, middle_initial: row.middle_initial, last_name: row.last_name, age: row.age, net_worth: row.net_worth})", entries);
  }

  @Override
  public void update(@NotNull Entry @NotNull ... entries) {
    alterEntries("UNWIND $batch AS row MATCH (e:Entry {id: row.id}) SET e.first_name = row.first_name, e.middle_initial = row.middle_initial, e.last_name = row.last_name, e.age = row.age, e.net_worth = row.net_worth", entries);
  }

  @Override
  public void remove(@NotNull Integer @NotNull ... ids) {
    try (Session session = driver.session(SessionConfig.forDatabase("neo4j"));
         Transaction tx = session.beginTransaction()) {
      List<Integer> idList = Arrays.asList(ids);
      tx.run("UNWIND $ids AS id MATCH (e:Entry {id: id}) DETACH DELETE e", Values.parameters("ids", idList));
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
  public Map<Integer, Entry> select() {
    Map<Integer, Entry> entries = new HashMap<>();
    try (Session session = driver.session(SessionConfig.forDatabase("neo4j"))) {
      Result result = session.run("MATCH (e:Entry) RETURN e.id AS id, e.first_name AS first_name, e.middle_initial AS middle_initial, e.last_name AS last_name, e.age AS age, e.net_worth AS net_worth");
      while (result.hasNext()) {
        Record record = result.next();
        int id = record.get("id").asInt();
        String firstName = record.get("first_name").asString();
        char middleInitial = record.get("middle_initial").asString().charAt(0);
        String lastName = record.get("last_name").asString();
        int age = record.get("age").asInt();
        double netWorth = record.get("net_worth").asDouble();
        entries.put(id, new Entry(id, firstName, middleInitial, lastName, age, netWorth));
      }
    }
    return entries;
  }

  private void alterEntries(String statement, Entry[] entries) {
    try (Session session = driver.session(SessionConfig.forDatabase("neo4j"));
         Transaction tx = session.beginTransaction()) {
      List<Value> records = new ArrayList<>();
      for (Entry entry : entries) {
        records.add(Values.parameters(
          "id", entry.getId(),
          "first_name", entry.getFirstName(),
          "middle_initial", String.valueOf(entry.getMiddleInitial()),
          "last_name", entry.getLastName(),
          "age", entry.getAge(),
          "net_worth", entry.getNetWorth()
        ));
      }
      tx.run(statement, Values.parameters("batch", records));
      tx.commit();
    }
  }

}