package me.jeremiah.databases.sql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import me.jeremiah.Entry;
import me.jeremiah.databases.Database;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractSQLDatabase implements Database {

  @Getter
  private final HikariConfig config;
  private HikariDataSource dataSource;

  public AbstractSQLDatabase(Class<? extends Driver> driver, String url) {
    try {
      DriverManager.registerDriver(driver.getConstructor().newInstance());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    config = new HikariConfig();
    config.setJdbcUrl(url);
    config.setAutoCommit(false);
    config.setMaximumPoolSize(20);
    config.setConnectionTimeout(30000);
    config.setIdleTimeout(600000);
    config.setMaxLifetime(1800000);
    config.setMinimumIdle(10);
    config.addDataSourceProperty("cachePrepStmts", "true");
    config.addDataSourceProperty("prepStmtCacheSize", "250");
    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
  }

  @Override
  public void open() {
    if (dataSource != null)
      throw new IllegalStateException("Database is already open");
    try {
      dataSource = new HikariDataSource(config);
      try (Connection connection = dataSource.getConnection();
           Statement statement = connection.createStatement()) {
        createTable(connection, statement);
        createIndex(connection, statement);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected void createTable(Connection connection, Statement statement) {
//    try {
//      statement.execute("DROP TABLE entries");
//      connection.commit();
//    } catch (SQLException e) {
//      System.out.println("Caught table entries doesn't exist exception, ignoring...");
//      System.out.println("Message: " + e.getMessage());
//    }
    try {
      statement.execute("CREATE TABLE entries(id INT PRIMARY KEY, first_name VARCHAR(32), middle_initial CHAR(1), last_name VARCHAR(32), age SMALLINT, net_worth FLOAT(53))");
      connection.commit();
    } catch (SQLException e) {
      System.out.println("Caught table entries already exists exception, ignoring...");
      System.out.println("Message: " + e.getMessage());
    }
  }

  private void createIndex(Connection connection, Statement statement) {
    try {
      statement.execute("CREATE INDEX idx_id ON entries(id)");
      connection.commit();
    } catch (SQLException e) {
      System.out.println("Caught index idx_id already exists exception, ignoring...");
    }
  }

  @Override
  public void close() {
    if (dataSource == null)
      throw new IllegalStateException("Database is already closed");
    dataSource.close();
    dataSource = null;
  }

  @Override
  public void wipe() {
    try (Connection connection = dataSource.getConnection();
         Statement statement = connection.createStatement()) {
      statement.execute("DELETE FROM entries");
      connection.commit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void insert(@NotNull Entry... entries) {
    final int batchSize = 1000;
    try (Connection connection = dataSource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO entries (id, first_name, middle_initial, last_name, age, net_worth) VALUES (?,?,?,?,?,?)")) {
      int count = 0;
      for (Entry entry : entries) {
        entry.serializeInsert(preparedStatement);
        preparedStatement.addBatch();
        if (++count % batchSize == 0) {
          preparedStatement.executeBatch();
        }
      }
      preparedStatement.executeBatch();
      connection.commit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void update(@NotNull Entry... entries) {
    final int batchSize = 1000;
    try (Connection connection = dataSource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement("UPDATE entries SET first_name = ?, middle_initial = ?, last_name = ?, age = ?, net_worth = ? WHERE id = ?")) {
      int count = 0;
      for (Entry entry : entries) {
        entry.serializeUpdate(preparedStatement);
        preparedStatement.addBatch();
        if (++count % batchSize == 0) {
          preparedStatement.executeBatch();
        }
      }
      preparedStatement.executeBatch();
      connection.commit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean exists(int id) {
    try (Connection connection = dataSource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM entries WHERE id = ?")) {
      preparedStatement.setInt(1, id);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        return resultSet.next();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void remove(int... ids) {
    final int batchSize = 1000;
    try (Connection connection = dataSource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM entries WHERE id = ?")) {
      int count = 0;
      for (int id : ids) {
        preparedStatement.setInt(1, id);
        preparedStatement.addBatch();
        if (++count % batchSize == 0)
          preparedStatement.executeBatch();
      }
      preparedStatement.executeBatch();
      connection.commit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<Integer, Entry> select() {
    Map<Integer, Entry> entries = new HashMap<>();

    try (Connection connection = dataSource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM entries")) {

      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        while (resultSet.next()) {
          int id = resultSet.getInt("id");
          entries.put(id, new Entry(id,
            resultSet.getString("first_name"),
            resultSet.getString("middle_initial").charAt(0),
            resultSet.getString("last_name"),
            resultSet.getInt("age"),
            resultSet.getDouble("net_worth")
          ));
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return entries;
  }

}