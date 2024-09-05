package me.jeremiah.databases.sql.local;

import me.jeremiah.databases.sql.AbstractSQLDatabase;

public class H2 extends AbstractSQLDatabase {

  public H2() {
    super(org.h2.Driver.class,
      "jdbc:h2:./databases/data.h2;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE");
  }

  @Override
  public String getName() {
    return "H2";
  }


}
