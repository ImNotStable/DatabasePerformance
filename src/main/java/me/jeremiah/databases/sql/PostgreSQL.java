package me.jeremiah.databases.sql;

public class PostgreSQL extends AbstractSQLDatabase {

  public PostgreSQL() {
    super(org.postgresql.Driver.class, "jdbc:postgresql://localhost:5432/data");
    getConfig().setUsername("postgres");
  }

}
