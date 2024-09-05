package me.jeremiah.databases.sql.local;

import me.jeremiah.databases.sql.AbstractSQLDatabase;

public class SQLite extends AbstractSQLDatabase {

  public SQLite() {
    super(org.sqlite.JDBC.class, "jdbc:sqlite:./databases/data.sqlite");
  }

  public String getName() {
    return "SQLite";
  }

}
