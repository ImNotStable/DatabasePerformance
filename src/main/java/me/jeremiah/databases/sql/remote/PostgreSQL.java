package me.jeremiah.databases.sql.remote;

import me.jeremiah.databases.sql.AbstractSQLDatabase;

public class PostgreSQL extends AbstractSQLDatabase {

  public PostgreSQL() {
    super(org.postgresql.Driver.class, "jdbc:postgresql://localhost:5432/data");
    getConfig().setUsername("postgres");
  }

  @Override
  public String getName() {
    return "PostgreSQL";
  }

}
