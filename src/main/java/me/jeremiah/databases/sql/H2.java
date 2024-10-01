package me.jeremiah.databases.sql;

public class H2 extends AbstractSQLDatabase {

  public H2() {
    super(org.h2.Driver.class, "jdbc:h2:file:./.databases/h2");
  }

}
