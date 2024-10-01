package me.jeremiah.databases.sql;

import me.jeremiah.Entry;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ByteSQLite extends SQLite {

  public ByteSQLite() {
    super();

    setCreateTableStatement("CREATE TABLE entries(id INT PRIMARY KEY, data BLOB)");
    setInsertEntryStatement("INSERT INTO entries (id, data) VALUES (?,?)");
    setUpdateEntryStatement("UPDATE entries SET data = ? WHERE id = ?");
    setSelectEntriesStatement("SELECT * FROM entries");
  }

  @Override
  public String getName() {
    return "Byte-SQLite";
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
  protected Entry deserializeEntry(int id, ResultSet resultSet) throws SQLException {
    return new Entry(id, resultSet.getBytes("data"));
  }

}
