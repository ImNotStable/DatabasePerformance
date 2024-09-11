package me.jeremiah;

import lombok.Getter;
import me.jeremiah.databases.Database;
import me.jeremiah.databases.nosql.Cassandra;
import me.jeremiah.databases.nosql.MongoDB;
import me.jeremiah.databases.nosql.Neo4j;
import me.jeremiah.databases.nosql.Redis;
import me.jeremiah.databases.sql.MariaDB;
import me.jeremiah.databases.sql.MicrosoftSQL;
import me.jeremiah.databases.sql.OracleDB;
import me.jeremiah.databases.sql.PostgreSQL;
import me.jeremiah.testing.DatabaseTester;

import java.util.List;

public class Main {

  @Getter
  private static final List<Database> databases = List.of(
    //new MySQL(),
    new MariaDB(),
    new PostgreSQL(),
    new OracleDB(),
    new MicrosoftSQL(),
    new MongoDB(),
    new Redis(),
    new Neo4j(),
    new Cassandra()
  );

  public static void main(String[] args) {
    DatabaseTester.testCluster(databases, 1_000, 10_000);
  }

}
