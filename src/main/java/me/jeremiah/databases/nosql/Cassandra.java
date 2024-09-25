package me.jeremiah.databases.nosql;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.primitives.Bytes;
import lombok.Getter;
import me.jeremiah.Entry;
import me.jeremiah.ExceptionManager;
import me.jeremiah.databases.Database;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class Cassandra implements Database {

  private static final int MAX_BATCH_SIZE = 190;
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
      .addTypeCodecs(new ByteArrayCodec())
      .buildAsync()
      .toCompletableFuture()
      .join();
    createTable();
  }

  private void createTable() {
    String dropTable = "DROP TABLE IF EXISTS data;";
    session.executeAsync(SimpleStatement.newInstance(dropTable)).toCompletableFuture().join();
    String tableCreation = "CREATE TABLE IF NOT EXISTS data (id int PRIMARY KEY, bytes blob);";
    session.executeAsync(SimpleStatement.newInstance(tableCreation)).toCompletableFuture().join();
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
      PreparedStatement insertStmt = session.prepareAsync("INSERT INTO data (id, bytes) VALUES (?, ?);").toCompletableFuture().join();
      for (int j = i; j < i + MAX_BATCH_SIZE && j < entries.length; j++) {
        Entry entry = entries[j];
        batchBuilder.addStatement(insertStmt.bind(entry.getId(), entry.bytes()));
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
      PreparedStatement updateStmt = session.prepareAsync("UPDATE data SET bytes = ? WHERE id = ?;").toCompletableFuture().join();
      for (int j = i; j < i + MAX_BATCH_SIZE && j < entries.length; j++) {
        Entry entry = entries[j];
        batchBuilder.addStatement(updateStmt.bind(entry.bytes(), entry.getId()));
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
    } catch (InterruptedException | ExecutionException exception) {
      ExceptionManager.handleException(this, exception);
      return false;
    }
  }

  @Override
  public void remove(int... ids) {
    List<CompletableFuture<AsyncResultSet>> futures = new ArrayList<>();
    for (int i = 0; i < ids.length; i += MAX_BATCH_SIZE) {
      BatchStatementBuilder batchBuilder = BatchStatement.builder(BatchType.UNLOGGED);
      PreparedStatement deleteStmt = session.prepareAsync("DELETE FROM data WHERE id = ?;").toCompletableFuture().join();
      for (int j = i; j < i + MAX_BATCH_SIZE && j < ids.length; j++)
        batchBuilder.addStatement(deleteStmt.bind(ids[j]));
      futures.add(session.executeAsync(batchBuilder.build()).toCompletableFuture());
    }
    futures.forEach(CompletableFuture::join);
  }

  @Override
  public Map<Integer, Entry> select() {
    Map<Integer, Entry> entries = new HashMap<>();
    PreparedStatement selectStmt = session.prepareAsync("SELECT id, bytes FROM data;").toCompletableFuture().join();
    CompletableFuture<AsyncResultSet> future = session.executeAsync(selectStmt.bind()).toCompletableFuture();
    try {
      AsyncResultSet resultSet = future.get();
      while (resultSet != null) {
        for (Row row : resultSet.currentPage()) {
          int id = row.getInt("id");
          byte[] bytes = row.getByteBuffer("bytes").array();
          entries.put(id, new Entry(id, bytes));
        }
        if (!resultSet.hasMorePages())
          break;
        resultSet = resultSet.fetchNextPage().toCompletableFuture().get();
      }
    } catch (InterruptedException | ExecutionException exception) {
      ExceptionManager.handleException(this, exception);
    }
    return entries;
  }

  @Getter
  private static class ByteArrayCodec implements TypeCodec<byte[]> {

    private final GenericType<byte[]> javaType = GenericType.of(byte[].class);
    private final DataType cqlType = TypeCodecs.BLOB.getCqlType();


    @Override
    public ByteBuffer encode(byte[] value, @NotNull ProtocolVersion protocolVersion) {
      return value == null ? null : ByteBuffer.wrap(value);
    }

    @Override
    public byte[] decode(ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
      return bytes == null ? null : bytes.array();
    }

    @Override
    public @NotNull String format(byte[] value) {
      return value == null ? "NULL" : Bytes.asList(value).toString();
    }

    @Override
    public byte[] parse(String value) {
      throw new UnsupportedOperationException("Parsing not supported for byte[]");
    }

  }

}