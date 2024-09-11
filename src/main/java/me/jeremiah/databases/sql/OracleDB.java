package me.jeremiah.databases.sql;

public class OracleDB extends AbstractSQLDatabase {

  public OracleDB() {
    super(oracle.jdbc.OracleDriver.class, "jdbc:oracle:thin:@localhost:1521/FREEPDB1");
    getConfig().setUsername("system");
    getConfig().setPassword("root");
  }

  @Override
  public String getName() {
    return "OracleDB";
  }

}
