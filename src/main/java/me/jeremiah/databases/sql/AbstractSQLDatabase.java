package me.jeremiah.databases.sql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import lombok.Setter;
import me.jeremiah.Entry;
import me.jeremiah.ExceptionManager;
import me.jeremiah.databases.Database;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractSQLDatabase implements Database {

  private static final int MAX_BATCH_SIZE = 1000;

  @Getter
  private final HikariConfig config;
  private HikariDataSource dataSource;

  // Statement Schema
  @Setter
  @Getter
  private String dropTable = "DROP TABLE entries";
  @Setter
  @Getter
  private String createTable = "CREATE TABLE entries(id INT PRIMARY KEY, first_name VARCHAR(32), middle_initial CHAR(1), last_name VARCHAR(32), age SMALLINT, net_worth FLOAT(53))";
  @Setter
  @Getter
  private String wipeTable = "DELETE FROM entries";
  @Setter
  @Getter
  private String insertEntry = "INSERT INTO entries (id, first_name, middle_initial, last_name, age, net_worth) VALUES (?,?,?,?,?,?)";
  @Setter
  @Getter
  private String updateEntry = "UPDATE entries SET first_name = ?, middle_initial = ?, last_name = ?, age = ?, net_worth = ? WHERE id = ?";
  @Setter
  @Getter
  private String entryExists = "SELECT * FROM entries WHERE id = ?";
  @Setter
  @Getter
  private String removeEntry = "DELETE FROM entries WHERE id = ?";
  @Setter
  @Getter
  private String selectEntries = "SELECT * FROM entries";

  public AbstractSQLDatabase(Class<? extends Driver> driver, String url) {
    try {
      DriverManager.registerDriver(driver.getConstructor().newInstance());
    } catch (Exception exception) {
      ExceptionManager.handleException(this, exception);
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
    dataSource = new HikariDataSource(config);
    try (Connection connection = dataSource.getConnection();
         Statement statement = connection.createStatement()) {
      reloadTable(connection, statement);
    } catch (SQLException exception) {
      ExceptionManager.handleException(this, exception);
    }
  }

  protected void reloadTable(Connection connection, Statement statement) {
    try {
      statement.execute(dropTable);
      connection.commit();
    } catch (SQLException exception) {
      ExceptionManager.handleException(this, exception);
    }
    try {
      statement.execute(createTable);
      connection.commit();
    } catch (SQLException exception) {
      ExceptionManager.handleException(this, exception);
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
    handle(wipeTable);
  }

  @Override
  public void insert(@NotNull Entry @NotNull ... entries) {
    handle(insertEntry, preparedStatement -> {
      int count = 0;
      for (Entry entry : entries) {
        parseInsert(entry, preparedStatement);
        preparedStatement.addBatch();
        if (++count % MAX_BATCH_SIZE == 0)
          preparedStatement.executeBatch();
      }
      preparedStatement.executeBatch();
    });
  }

  @Override
  public void update(@NotNull Entry @NotNull ... entries) {
    handle(updateEntry, preparedStatement -> {
      int count = 0;
      for (Entry entry : entries) {
        parseUpdate(entry, preparedStatement);
        preparedStatement.addBatch();
        if (++count % MAX_BATCH_SIZE == 0)
          preparedStatement.executeBatch();
      }
      preparedStatement.executeBatch();
    });
  }

  @Override
  public boolean exists(int id) {
    AtomicBoolean exists = new AtomicBoolean(false);
    handleQuery(entryExists, preparedStatement -> {
      parseExists(id, preparedStatement);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        exists.set(resultSet.next());
      }
    });
    return exists.get();
  }

  @Override
  public void remove(@NotNull Integer @NotNull ... ids) {
    handle(removeEntry, preparedStatement -> {
      int count = 0;
      for (int id : ids) {
        parseRemove(id, preparedStatement);
        preparedStatement.addBatch();
        if (++count % MAX_BATCH_SIZE == 0)
          preparedStatement.executeBatch();
      }
      preparedStatement.executeBatch();
    });
  }

  @Override
  public Map<Integer, Entry> select() {
    Map<Integer, Entry> entries = new HashMap<>();

    handleQuery(selectEntries, preparedStatement -> {
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        while (resultSet.next()) {
          int id = resultSet.getInt("id");
          entries.put(id, deserializeEntry(id, resultSet));
        }
      }
    });

    return entries;
  }
  protected void parseInsert(Entry entry, PreparedStatement preparedStatement) throws SQLException {
    preparedStatement.setInt(1, entry.getId());
    preparedStatement.setString(2, entry.getFirstName());
    preparedStatement.setString(3, String.valueOf(entry.getMiddleInitial()));
    preparedStatement.setString(4, entry.getLastName());
    preparedStatement.setInt(5, entry.getAge());
    preparedStatement.setDouble(6, entry.getNetWorth());
  }

  protected void parseUpdate(Entry entry, PreparedStatement preparedStatement) throws SQLException {
    preparedStatement.setString(1, entry.getFirstName());
    preparedStatement.setString(2, String.valueOf(entry.getMiddleInitial()));
    preparedStatement.setString(3, entry.getLastName());
    preparedStatement.setInt(4, entry.getAge());
    preparedStatement.setDouble(5, entry.getNetWorth());
    preparedStatement.setInt(6, entry.getId());
  }

  protected void parseExists(int id, PreparedStatement preparedStatement) throws SQLException {
    preparedStatement.setInt(1, id);
  }

  protected void parseRemove(int id, PreparedStatement preparedStatement) throws SQLException {
    preparedStatement.setInt(1, id);
  }

  private void handle(String statement) {
    handle(statement, PreparedStatement::execute);
  }

  private void handle(String statement, SQLAction action) {
    try (Connection connection = dataSource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement(statement)) {
      action.accept(preparedStatement);
      connection.commit();
    } catch (SQLException exception) {
      ExceptionManager.handleException(this, exception);
    }
  }

  private void handleQuery(String statement, SQLAction action) {
    try (Connection connection = dataSource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement(statement)) {
      action.accept(preparedStatement);
    } catch (SQLException exception) {
      ExceptionManager.handleException(this, exception);
    }
  }

  protected Entry deserializeEntry(int id, ResultSet resultSet) {
    try {
      return new Entry(id,
        resultSet.getString("first_name"),
        resultSet.getString("middle_initial").charAt(0),
        resultSet.getString("last_name"),
        resultSet.getInt("age"),
        resultSet.getDouble("net_worth")
      );
    } catch (SQLException exception) {
      ExceptionManager.handleException(this, exception);
      return null;
    }
  }

  private interface SQLAction {

    void accept(PreparedStatement preparedStatement) throws SQLException;

  }

}