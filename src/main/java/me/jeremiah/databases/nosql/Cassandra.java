package me.jeremiah.databases.nosql;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.*;
import me.jeremiah.Entry;
import me.jeremiah.databases.Database;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class Cassandra implements Database {

  private static final int MAX_BATCH_SIZE = 150;
  private AsyncCqlSession session;

  @Override
  public String getName() {
    return "Cassandra";
  }

  @Override
  public void open() {
    ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder = DriverConfigLoader.programmaticBuilder()
      .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
      .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 50)
      .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, 1024);

    session = CqlSession.builder()
      .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
      .withLocalDatacenter("datacenter1")
      .withKeyspace(CqlIdentifier.fromCql("data"))
      .withConfigLoader(configLoaderBuilder.build())
      .buildAsync()
      .toCompletableFuture()
      .join();
    createTable();
  }

  private void createTable() {
    //String drop = "DROP TABLE IF EXISTS data;";
    //session.executeAsync(SimpleStatement.newInstance(drop)).toCompletableFuture().join();
    String query = "CREATE TABLE IF NOT EXISTS data (id int PRIMARY KEY, firstName text, middleInitial text, lastName text, age int, netWorth double);";
    session.executeAsync(SimpleStatement.newInstance(query)).toCompletableFuture().join();
  }

  @Override
  public void close() {
    if (session != null)
      session.closeAsync().toCompletableFuture().join();
  }

  @Override
  public void wipe() {
    session.executeAsync(SimpleStatement.newInstance("TRUNCATE data;")).toCompletableFuture().join();
  }

  @Override
  public void insert(@NotNull Entry... entries) {
    List<CompletableFuture<AsyncResultSet>> futures = new ArrayList<>();
    for (int i = 0; i < entries.length; i += MAX_BATCH_SIZE) {
      BatchStatementBuilder batchBuilder = BatchStatement.builder(BatchType.UNLOGGED);
      PreparedStatement insertStmt = session.prepareAsync("INSERT INTO data (id, firstName, middleInitial, lastName, age, netWorth) VALUES (?, ?, ?, ?, ?, ?);").toCompletableFuture().join();
      for (int j = i; j < i + MAX_BATCH_SIZE && j < entries.length; j++) {
        Entry entry = entries[j];
        batchBuilder.addStatement(insertStmt.bind(entry.getId(), entry.getFirstName(), String.valueOf(entry.getMiddleInitial()), entry.getLastName(), entry.getAge(), entry.getNetWorth()));
      }
      futures.add(session.executeAsync(batchBuilder.build()).toCompletableFuture());
    }
    futures.forEach(CompletableFuture::join);
  }

  @Override
  public void update(@NotNull Entry... entries) {
    List<CompletableFuture<AsyncResultSet>> futures = new ArrayList<>();
    for (int i = 0; i < entries.length; i += MAX_BATCH_SIZE) {
      BatchStatementBuilder batchBuilder = BatchStatement.builder(BatchType.UNLOGGED);
      PreparedStatement updateStmt = session.prepareAsync("UPDATE data SET firstName = ?, middleInitial = ?, lastName = ?, age = ?, netWorth = ? WHERE id = ?;").toCompletableFuture().join();
      for (int j = i; j < i + MAX_BATCH_SIZE && j < entries.length; j++) {
        Entry entry = entries[j];
        batchBuilder.addStatement(updateStmt.bind(entry.getFirstName(), String.valueOf(entry.getMiddleInitial()), entry.getLastName(), entry.getAge(), entry.getNetWorth(), entry.getId()));
      }
      futures.add(session.executeAsync(batchBuilder.build()).toCompletableFuture());
    }
    futures.forEach(CompletableFuture::join);
  }

  @Override
  public boolean exists(int id) {
    PreparedStatement existsStmt = session.prepareAsync("SELECT COUNT(*) FROM data WHERE id = ?;").toCompletableFuture().join();
    CompletableFuture<AsyncResultSet> future = session.executeAsync(existsStmt.bind(id)).toCompletableFuture();
    try {
      AsyncResultSet resultSet = future.get();
      Row row = resultSet.one();
      return row != null && row.getLong(0) > 0;
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      return false;
    }
  }

  @Override
  public void remove(int... ids) {
    List<CompletableFuture<AsyncResultSet>> futures = new ArrayList<>();
    for (int i = 0; i < ids.length; i += MAX_BATCH_SIZE) {
      BatchStatementBuilder batchBuilder = BatchStatement.builder(BatchType.UNLOGGED);
      PreparedStatement deleteStmt = session.prepareAsync("DELETE FROM data WHERE id = ?;").toCompletableFuture().join();
      for (int j = i; j < i + MAX_BATCH_SIZE && j < ids.length; j++) {
        batchBuilder.addStatement(deleteStmt.bind(ids[j]));
      }
      futures.add(session.executeAsync(batchBuilder.build()).toCompletableFuture());
    }
    futures.forEach(CompletableFuture::join);
  }

  @Override
  public Map<Integer, Entry> select() {
    Map<Integer, Entry> entries = new HashMap<>();
    PreparedStatement selectStmt = session.prepareAsync("SELECT id, firstName, middleInitial, lastName, age, netWorth FROM data;").toCompletableFuture().join();
    CompletableFuture<AsyncResultSet> future = session.executeAsync(selectStmt.bind()).toCompletableFuture();
    try {
      AsyncResultSet resultSet = future.get();
      while (resultSet != null) {
        for (Row row : resultSet.currentPage()) {
          int id = row.getInt("id");
          String firstName = row.getString("firstName");
          char middleInitial = row.getString("middleInitial").charAt(0);
          String lastName = row.getString("lastName");
          int age = row.getInt("age");
          double netWorth = row.getDouble("netWorth");
          entries.put(id, new Entry(id, firstName, middleInitial, lastName, age, netWorth));
        }
        if (resultSet.hasMorePages()) {
          resultSet = resultSet.fetchNextPage().toCompletableFuture().get();
        } else {
          resultSet = null;
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    return entries;
  }

}