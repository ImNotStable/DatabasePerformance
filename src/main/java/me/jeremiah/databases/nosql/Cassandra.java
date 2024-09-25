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
    handleBatchAction("INSERT INTO data (id, bytes) VALUES (?, ?);", entries,
      entry -> new Object[]{entry.getId(), entry.bytes()});
  }

  @Override
  public void update(@NotNull Entry @NotNull ... entries) {
    handleBatchAction("UPDATE data SET bytes = ? WHERE id = ?;", entries,
      entry -> new Object[]{entry.bytes(), entry.getId()});
  }

  @Override
  public void remove(@NotNull Integer @NotNull ... ids) {
    handleBatchAction("DELETE FROM data WHERE id = ?;", ids, id -> new Object[]{id});
  }

  @Override
  public boolean exists(int id) {
    return handleQuery("SELECT COUNT(*) FROM data WHERE id = ?;", resultSet -> {
      Row row = resultSet.one();
      return row != null && row.getLong(0) > 0;
    }, id);
  }

  @Override
  public Map<Integer, Entry> select() {
    return handleQuery("SELECT id, bytes FROM data;", resultSet -> {
      Map<Integer, Entry> entries = new HashMap<>();
        for (Row row : resultSet) {
          int id = row.getInt("id");
          byte[] bytes = row.getByteBuffer("bytes").array();
          entries.put(id, new Entry(id, bytes));
        }
        return entries;
      });
  }

  private <W> void handleBatchAction(String statement, W[] writables, BatchAction<W> parser) {
    for (int i = 0; i < writables.length; i += BATCH_SIZE) {
      BatchStatementBuilder batchBuilder = BatchStatement.builder(BatchType.UNLOGGED);
      PreparedStatement preparedStatement = session.prepare(statement);
      for (int j = i; j < i + BATCH_SIZE && j < writables.length; j++)
        batchBuilder.addStatement(preparedStatement.bind(parser.accept(writables[j])));
      session.execute(batchBuilder.build());
    }
  }

  private <R> R handleQuery(String statement, Query<R> query, Object... bindables) {
    PreparedStatement preparedStatement = session.prepare(statement);
    ResultSet resultSet = session.execute(preparedStatement.bind(bindables));
    return query.apply(resultSet);
  }

  private interface BatchAction<W> {

    Object[] accept(W writable);

  }

  private interface Query<R> {

    R apply(ResultSet resultSet);

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