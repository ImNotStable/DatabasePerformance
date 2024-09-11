package me.jeremiah.databases.sql;

public class MariaDB extends AbstractSQLDatabase {

  public MariaDB() {
    super(org.mariadb.jdbc.Driver.class, "jdbc:mariadb://localhost:3307/data");
    getConfig().setUsername("root");
  }

  @Override
  public String getName() {
    return "MariaDB";
  }

}
