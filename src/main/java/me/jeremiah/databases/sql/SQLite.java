package me.jeremiah.databases.sql;

public class SQLite extends AbstractSQLDatabase {

  public SQLite() {
    super(org.sqlite.JDBC.class, "jdbc:sqlite:file:./.databases/sqlite");
  }

}
