package me.jeremiah.databases.sql;

public class CockroachDB extends AbstractSQLDatabase {

  public CockroachDB() {
    super(org.postgresql.Driver.class, "jdbc:postgresql://localhost:26257/data");
    getConfig().setUsername("root");
  }

  @Override
  public String getName() {
    return "CockroachDB";
  }

}
