package me.jeremiah.databases.sql;

import me.jeremiah.Entry;
import me.jeremiah.ExceptionManager;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ByteOracleDB extends OracleDB {

  public ByteOracleDB() {
    super();

    setCreateTable("CREATE TABLE entries(id INT PRIMARY KEY, data BLOB)");
    setInsertEntry("INSERT INTO entries (id, data) VALUES (?,?)");
    setUpdateEntry("UPDATE entries SET data = ? WHERE id = ?");
    setSelectEntries("SELECT * FROM entries");
  }

  @Override
  public String getName() {
    return "Byte-OracleDB";
  }

  @Override
  protected void parseInsert(Entry entry, PreparedStatement preparedStatement) {
    try {
      preparedStatement.setInt(1, entry.getId());
      preparedStatement.setBytes(2, entry.bytes());
    } catch (SQLException e) {
      ExceptionManager.handleException(this, e);
    }
  }

  @Override
  protected void parseUpdate(Entry entry, PreparedStatement preparedStatement) {
    try {
      preparedStatement.setBytes(1, entry.bytes());
      preparedStatement.setInt(2, entry.getId());
    } catch (SQLException e) {
      ExceptionManager.handleException(this, e);
    }
  }

  @Override
  protected void parseExists(int id, PreparedStatement preparedStatement) {
    try {
      preparedStatement.setInt(1, id);
    } catch (SQLException e) {
      ExceptionManager.handleException(this, e);
    }
  }

  @Override
  protected void parseRemove(int id, PreparedStatement preparedStatement) {
    try {
      preparedStatement.setInt(1, id);
    } catch (SQLException e) {
      ExceptionManager.handleException(this, e);
    }
  }

  @Override
  protected Entry deserializeEntry(int id, ResultSet resultSet) {
    try {
      return new Entry(id, resultSet.getBytes("data"));
    } catch (SQLException e) {
      ExceptionManager.handleException(this, e);
      return null;
    }
  }


}
