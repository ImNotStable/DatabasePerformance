package me.jeremiah.databases.sql;

public class MySQL extends AbstractSQLDatabase {

  public MySQL() {
    super(com.mysql.cj.jdbc.Driver.class, "jdbc:mysql://localhost:3306/data");
    getConfig().setUsername("root");
  }

}
