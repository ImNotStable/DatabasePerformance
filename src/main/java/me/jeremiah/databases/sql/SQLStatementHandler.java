package me.jeremiah.databases.sql;

import lombok.Getter;
import lombok.Setter;
import me.jeremiah.Entry;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Setter
@Getter
class SQLStatementHandler {

  private String createTableStatement = "CREATE TABLE entries(id INT PRIMARY KEY, first_name VARCHAR(32) NOT NULL, middle_initial CHAR(1) NOT NULL, last_name VARCHAR(32) NOT NULL, age SMALLINT NOT NULL, net_worth FLOAT(53) NOT NULL)";
  private String dropTableStatement = "DROP TABLE entries";

  private String tableWipeStatement = "DELETE FROM entries";

  private String insertEntryStatement = "INSERT INTO entries (id, first_name, middle_initial, last_name, age, net_worth) VALUES (?,?,?,?,?,?)";
  private String updateEntryStatement = "UPDATE entries SET first_name = ?, middle_initial = ?, last_name = ?, age = ?, net_worth = ? WHERE id = ?";
  private String removeEntryStatement = "DELETE FROM entries WHERE id = ?";

  private String entryExistsStatement = "SELECT * FROM entries WHERE id = ?";
  private String selectEntriesStatement = "SELECT * FROM entries";

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

  protected void parseRemove(int id, PreparedStatement preparedStatement) throws SQLException {
    preparedStatement.setInt(1, id);
  }

  protected void parseExists(int id, PreparedStatement preparedStatement) throws SQLException {
    preparedStatement.setInt(1, id);
  }

  protected Entry deserializeEntry(int id, ResultSet resultSet) throws SQLException {
    return new Entry(id,
      resultSet.getString("first_name"),
      resultSet.getString("middle_initial").charAt(0),
      resultSet.getString("last_name"),
      resultSet.getInt("age"),
      resultSet.getDouble("net_worth")
    );
  }

}
