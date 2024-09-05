package me.jeremiah.databases.sql.remote;

import me.jeremiah.databases.sql.AbstractSQLDatabase;

public class OracleDB extends AbstractSQLDatabase {

  public OracleDB() {
    super(oracle.jdbc.OracleDriver.class, "jdbc:oracle:thin:@localhost:1521/FREEPDB1");
    setUsername("system");
    setPassword("root");
  }

  @Override
  public String getName() {
    return "OracleDB";
  }

}
