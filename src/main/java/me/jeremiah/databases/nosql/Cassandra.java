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
import me.jeremiah.databases.Database;
import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class Cassandra implements Database {

  private static final int BATCH_SIZE = 200;
  private CqlSession session;

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
      .build();
    createTable();
  }

  private void createTable() {
    String dropTable = "DROP TABLE IF EXISTS data;";
    session.execute(dropTable);
    String tableCreation = "CREATE TABLE IF NOT EXISTS data (id int PRIMARY KEY, bytes blob);";
    session.execute(tableCreation);
  }

  @Override
  public void close() {
    if (session != null)
      session.close();
  }

  @Override
  public void wipe() {
    session.execute("TRUNCATE data;");
  }

  @Override
  public void insert(@NotNull Entry @NotNull ... entries) {
    for (int i = 0; i < entries.length; i += BATCH_SIZE) {
      BatchStatementBuilder batchBuilder = BatchStatement.builder(BatchType.UNLOGGED);
      PreparedStatement insertStmt = session.prepare("INSERT INTO data (id, bytes) VALUES (?, ?);");
      for (int j = i; j < i + BATCH_SIZE && j < entries.length; j++) {
        Entry entry = entries[j];
        batchBuilder.addStatement(insertStmt.bind(entry.getId(), entry.bytes()));
      }
      session.execute(batchBuilder.build());
    }
  }

  @Override
  public void update(@NotNull Entry @NotNull ... entries) {
    for (int i = 0; i < entries.length; i += BATCH_SIZE) {
      BatchStatementBuilder batchBuilder = BatchStatement.builder(BatchType.UNLOGGED);
      PreparedStatement updateStmt = session.prepare("UPDATE data SET bytes = ? WHERE id = ?;");
      for (int j = i; j < i + BATCH_SIZE && j < entries.length; j++) {
        Entry entry = entries[j];
        batchBuilder.addStatement(updateStmt.bind(entry.bytes(), entry.getId()));
      }
      session.execute(batchBuilder.build());
    }
  }

  @Override
  public boolean exists(int id) {
    PreparedStatement existsStmt = session.prepare("SELECT COUNT(*) FROM data WHERE id = ?;");
    ResultSet resultSet = session.execute(existsStmt.bind(id));
    Row row = resultSet.one();
    return row != null && row.getLong(0) > 0;
  }

  @Override
  public void remove(@NotNull Integer @NotNull ... ids) {
    for (int i = 0; i < ids.length; i += BATCH_SIZE) {
      BatchStatementBuilder batchBuilder = BatchStatement.builder(BatchType.UNLOGGED);
      PreparedStatement deleteStmt = session.prepare("DELETE FROM data WHERE id = ?;");
      for (int j = i; j < i + BATCH_SIZE && j < ids.length; j++)
        batchBuilder.addStatement(deleteStmt.bind(ids[j]));
      session.execute(batchBuilder.build());
    }
  }

  @Override
  public Map<Integer, Entry> select() {
    Map<Integer, Entry> entries = new HashMap<>();
    PreparedStatement selectStmt = session.prepare("SELECT id, bytes FROM data;");
    ResultSet resultSet = session.execute(selectStmt.bind());
    for (Row row : resultSet) {
      int id = row.getInt("id");
      byte[] bytes = row.getByteBuffer("bytes").array();
      entries.put(id, new Entry(id, bytes));
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