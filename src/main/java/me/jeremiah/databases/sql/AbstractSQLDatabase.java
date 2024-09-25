package me.jeremiah.databases.sql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import me.jeremiah.Entry;
import me.jeremiah.ExceptionManager;
import me.jeremiah.databases.Database;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractSQLDatabase extends SQLStatementHandler implements Database {

  private static final int MAX_BATCH_SIZE = 1000;

  @Getter
  private final HikariConfig config;
  private HikariDataSource dataSource;

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
      statement.execute(getDropTableStatement());
      connection.commit();
    } catch (SQLException exception) {
      ExceptionManager.handleException(this, exception);
    }
    try {
      statement.execute(getCreateTableStatement());
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
    handle(getTableWipeStatement());
  }

  @Override
  public void insert(@NotNull Entry @NotNull ... entries) {
    handleBatchAction(getInsertEntryStatement(), entries, this::parseInsert);
  }

  @Override
  public void update(@NotNull Entry @NotNull ... entries) {
    handleBatchAction(getUpdateEntryStatement(), entries, this::parseUpdate);
  }

  @Override
  public void remove(@NotNull Integer @NotNull ... ids) {
    handleBatchAction(getRemoveEntryStatement(), ids, this::parseRemove);
  }

  @Override
  public boolean exists(int id) {
    return Boolean.TRUE.equals(handleQuery(getEntryExistsStatement(),
      preparedStatement -> parseExists(id, preparedStatement),
      ResultSet::next
    ));
  }

  @Override
  public Map<Integer, Entry> select() {
    return handleQuery(getSelectEntriesStatement(), resultSet -> {
      Map<Integer, Entry> entries = new HashMap<>();
      while (resultSet.next()) {
        int id = resultSet.getInt("id");
        entries.put(id, deserializeEntry(id, resultSet));
      }
      return entries;
    });
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

  private <W> void handleBatchAction(String statement, W[] writables, SQLBatchAction<W> parser) {
    handle(statement, preparedStatement -> {
      int count = 0;
      for (W writable : writables) {
        parser.accept(writable, preparedStatement);
        preparedStatement.addBatch();
        if (++count % MAX_BATCH_SIZE == 0)
          preparedStatement.executeBatch();
      }
      preparedStatement.executeBatch();
    });
  }

  private <R> R handleQuery(String statement, SQLQuery<R> query) {
    return handleQuery(statement, null, query);
  }

  private <R> R handleQuery(String statement, SQLAction action, SQLQuery<R> query) {
    try (Connection connection = dataSource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement(statement)) {
      if (action != null)
        action.accept(preparedStatement);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        return query.apply(resultSet);
      }
    } catch (SQLException exception) {
      ExceptionManager.handleException(this, exception);
      return null;
    }
  }

  private interface SQLAction {

    void accept(PreparedStatement preparedStatement) throws SQLException;

  }

  private interface SQLBatchAction<W> {

    void accept(W writable, PreparedStatement preparedStatement) throws SQLException;

  }

  private interface SQLQuery<R> {

    R apply(ResultSet resultSet) throws SQLException;

  }

}