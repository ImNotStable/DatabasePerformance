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
  }

  protected void setUsername(String username) {
    config.setUsername(username);
  }

  protected void setPassword(String password) {
    config.setPassword(password);
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
        createIndex(connection, statement, "id");
        createIndex(connection, statement, "first_name");
        createIndex(connection, statement, "middle_initial");
        createIndex(connection, statement, "last_name");
      }
    } catch (Exception exception) {
      throw new RuntimeException(exception);
    }
  }

  protected void createTable(Connection connection, Statement statement) throws Exception {
    statement.execute("CREATE TABLE IF NOT EXISTS entries(id INT PRIMARY KEY, first_name VARCHAR(32), middle_initial CHAR(1), last_name VARCHAR(32))");
    connection.commit();
  }

  private void createIndex(Connection connection, Statement statement, String column) {
    try {
      statement.execute("CREATE INDEX idx_" + column + " ON entries(" + column + ")");
      connection.commit();
    } catch (SQLException e) {
      System.out.println("Caught index idx_" + column + " already exist exception, ignoring...");
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
    try (Connection connection = dataSource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO entries (id, first_name, middle_initial, last_name) VALUES (?,?,?,?)")) {
      for (Entry entry : entries) {
        entry.serializeInsert(preparedStatement);
        preparedStatement.addBatch();
      }
      preparedStatement.executeBatch();
      connection.commit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void update(@NotNull Entry... entries) {
    try (Connection connection = dataSource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement("UPDATE entries SET first_name = ?, middle_initial = ?, last_name = ? WHERE id = ?")) {
      for (Entry entry : entries) {
        entry.serializeUpdate(preparedStatement);
        preparedStatement.addBatch();
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
    try (Connection connection = dataSource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement("DELETE FROM entries WHERE id = ?")) {
      for (int id : ids) {
        preparedStatement.setInt(1, id);
        preparedStatement.addBatch();
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
            resultSet.getString("last_name")
          ));
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return entries;
  }

}
