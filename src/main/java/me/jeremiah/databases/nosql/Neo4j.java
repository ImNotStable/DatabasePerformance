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
    driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.none());
    try (Session session = driver.session()) {
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
    try (Session session = driver.session()) {
      session.run("MATCH (n) DETACH DELETE n");
    }
  }

  @Override
  public void insert(@NotNull Entry... entries) {
    try (Session session = driver.session()) {
      try (Transaction tx = session.beginTransaction()) {
        List<Value> records = new ArrayList<>();
        for (Entry entry : entries) {
          records.add(Values.parameters("id", entry.getId(), "firstName", entry.getFirstName(), "middleInitial", String.valueOf(entry.getMiddleInitial()), "lastName", entry.getLastName()));
        }
        tx.run("UNWIND $batch AS row CREATE (e:Entry {id: row.id, firstName: row.firstName, middleInitial: row.middleInitial, lastName: row.lastName})", Values.parameters("batch", records));
        tx.commit();
      }
    }
  }

  @Override
  public void update(@NotNull Entry... entries) {
    try (Session session = driver.session()) {
      try (Transaction tx = session.beginTransaction()) {
        List<Value> records = new ArrayList<>();
        for (Entry entry : entries) {
          records.add(Values.parameters("id", entry.getId(), "firstName", entry.getFirstName(), "middleInitial", String.valueOf(entry.getMiddleInitial()), "lastName", entry.getLastName()));
        }
        tx.run("UNWIND $batch AS row MATCH (e:Entry {id: row.id}) SET e.firstName = row.firstName, e.middleInitial = row.middleInitial, e.lastName = row.lastName", Values.parameters("batch", records));
        tx.commit();
      }
    }
  }

  @Override
  public boolean exists(int id) {
    try (Session session = driver.session()) {
      Result result = session.run("PROFILE MATCH (e:Entry {id: $id}) RETURN e", Values.parameters("id", id));
      return result.hasNext();
    }
  }

  @Override
  public void remove(int... ids) {
    try (Session session = driver.session()) {
      try (Transaction tx = session.beginTransaction()) {
        List<Integer> idList = new ArrayList<>();
        for (int id : ids) {
          idList.add(id);
        }
        tx.run("UNWIND $ids AS id MATCH (e:Entry {id: id}) DETACH DELETE e", Values.parameters("ids", idList));
        tx.commit();
      }
    }
  }

  @Override
  public Map<Integer, Entry> select() {
    Map<Integer, Entry> entries = new HashMap<>();
    try (Session session = driver.session()) {
      Result result = session.run("PROFILE MATCH (e:Entry) RETURN e.id AS id, e.firstName AS firstName, e.middleInitial AS middleInitial, e.lastName AS lastName");
      while (result.hasNext()) {
        Record record = result.next();
        int id = record.get("id").asInt();
        entries.put(id, new Entry(id, record.get("firstName").asString(), record.get("middleInitial").asString().charAt(0), record.get("lastName").asString()));
      }
    }
    return entries;
  }
}