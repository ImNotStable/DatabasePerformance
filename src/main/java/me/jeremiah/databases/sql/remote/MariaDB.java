package me.jeremiah.databases.sql.remote;

import me.jeremiah.databases.sql.AbstractSQLDatabase;

public class MariaDB extends AbstractSQLDatabase {

  public MariaDB() {
    super(org.mariadb.jdbc.Driver.class, "jdbc:mariadb://localhost:3307/data");
    setUsername("root");
  }

  @Override
  public String getName() {
    return "MariaDB";
  }

}
