package me.jeremiah.databases.sql;

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

}
