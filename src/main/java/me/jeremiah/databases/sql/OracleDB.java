package me.jeremiah.databases.sql;

import me.jeremiah.Entry;
import me.jeremiah.ExceptionManager;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class OracleDB extends AbstractSQLDatabase {

  public OracleDB() {
    super(oracle.jdbc.OracleDriver.class, "jdbc:oracle:thin:@localhost:1521/FREEPDB1");
    getConfig().setUsername("system");
    getConfig().setPassword("root");

    setCreateTable("CREATE TABLE entries(id INT PRIMARY KEY, first_name VARCHAR2(32), middle_initial CHAR(1), last_name VARCHAR2(32), age INT, net_worth BINARY_DOUBLE)");
  }

  @Override
  public String getName() {
    return "OracleDB";
  }

  @Override
  protected void parseInsert(Entry entry, PreparedStatement preparedStatement) throws SQLException {
    preparedStatement.setInt(1, entry.getId());
    preparedStatement.setString(2, entry.getFirstName());
    preparedStatement.setString(3, String.valueOf(entry.getMiddleInitial()));
    preparedStatement.setString(4, entry.getLastName());
    preparedStatement.setInt(5, entry.getAge());
    preparedStatement.setBigDecimal(6, BigDecimal.valueOf(entry.getNetWorth()));
  }

  @Override
  protected void parseUpdate(Entry entry, PreparedStatement preparedStatement) throws SQLException {
    preparedStatement.setString(1, entry.getFirstName());
    preparedStatement.setString(2, String.valueOf(entry.getMiddleInitial()));
    preparedStatement.setString(3, entry.getLastName());
    preparedStatement.setInt(4, entry.getAge());
    preparedStatement.setBigDecimal(5, BigDecimal.valueOf(entry.getNetWorth()));
    preparedStatement.setInt(6, entry.getId());
  }

  @Override
  protected Entry deserializeEntry(int id, ResultSet resultSet) {
    try {
      return new Entry(
        id,
        resultSet.getString("first_name"),
        resultSet.getString("middle_initial").charAt(0),
        resultSet.getString("last_name"),
        resultSet.getInt("age"),
        resultSet.getBigDecimal("net_worth").doubleValue()
      );
    } catch (SQLException exception) {
      ExceptionManager.handleException(this, exception);
    }
    return null;
  }

}
