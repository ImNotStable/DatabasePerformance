package me.jeremiah.databases.sql.remote;

import me.jeremiah.databases.sql.AbstractSQLDatabase;

import java.sql.Connection;
import java.sql.Statement;

public class MicrosoftSQL extends AbstractSQLDatabase {

  public MicrosoftSQL() {
    super(com.microsoft.sqlserver.jdbc.SQLServerDriver.class, "jdbc:sqlserver://localhost:1433;databaseName=data;encrypt=false;trustServerCertificate=true");
    getConfig().setUsername("sa");
    getConfig().setPassword("yourStrong(!)Password");
  }

  @Override
  public String getName() {
    return "MicrosoftSQL";
  }

  @Override
  protected void createTable(Connection connection, Statement statement) throws Exception {
    // Drop the table if it exists in SQL Server
    statement.execute("IF OBJECT_ID('entries', 'U') IS NOT NULL DROP TABLE entries;");
    connection.commit();

    // Create the table if it doesn't exist in SQL Server
    statement.execute("CREATE TABLE entries (" +
      "id INT PRIMARY KEY, " +
      "first_name VARCHAR(32), " +
      "middle_initial CHAR(1), " +
      "last_name VARCHAR(32)" +
      ");");
    connection.commit();
  }

}
