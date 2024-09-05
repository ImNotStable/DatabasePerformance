package me.jeremiah.databases.sql.remote;

import me.jeremiah.databases.sql.AbstractSQLDatabase;

public class MySQL extends AbstractSQLDatabase {

  public MySQL() {
    super(com.mysql.cj.jdbc.Driver.class, "jdbc:mysql://localhost:3306/data");
    setUsername("root");
  }

  @Override
  public String getName() {
    return "MySQL";
  }

}
