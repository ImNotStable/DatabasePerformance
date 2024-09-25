package me.jeremiah.databases.sql;

import me.jeremiah.Entry;
import me.jeremiah.ExceptionManager;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ByteMicrosoftSQL extends MicrosoftSQL {

  public ByteMicrosoftSQL() {
    super();

    setCreateTable("CREATE TABLE entries(id INT PRIMARY KEY, data BINARY(40))");
    setInsertEntry("INSERT INTO entries (id, data) VALUES (?,?)");
    setUpdateEntry("UPDATE entries SET data = ? WHERE id = ?");
    setSelectEntries("SELECT * FROM entries");
  }

  @Override
  public String getName() {
    return "Byte-MicrosoftSQL";
  }

  @Override
  protected void parseInsert(Entry entry, PreparedStatement preparedStatement) throws SQLException {
    preparedStatement.setInt(1, entry.getId());
    preparedStatement.setBytes(2, entry.bytes());
  }

  @Override
  protected void parseUpdate(Entry entry, PreparedStatement preparedStatement) throws SQLException {
    preparedStatement.setBytes(1, entry.bytes());
    preparedStatement.setInt(2, entry.getId());
  }

  @Override
  protected Entry deserializeEntry(int id, ResultSet resultSet) {
    try {
      return new Entry(id, resultSet.getBytes("data"));
    } catch (SQLException exception) {
      ExceptionManager.handleException(this, exception);
      return null;
    }
  }


}
