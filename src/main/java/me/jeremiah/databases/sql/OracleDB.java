package me.jeremiah.databases.sql;

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

}
