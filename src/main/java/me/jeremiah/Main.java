package me.jeremiah;

import me.jeremiah.databases.Database;
import me.jeremiah.databases.nosql.Cassandra;
import me.jeremiah.databases.nosql.MongoDB;
import me.jeremiah.databases.nosql.Neo4j;
import me.jeremiah.databases.nosql.Redis;
import me.jeremiah.databases.sql.*;
import me.jeremiah.testing.TestCluster;

import java.util.List;

public final class Main {

  public static void main(String[] args) {
    TestCluster cluster = TestCluster.test(getDatabaseStack(), 1_000, 10_000);
    cluster.start();
    cluster.createLog();
  }

  public static List<Database> getDatabaseStack() {
    return List.of(
      new ByteMariaDB(),
      new BytePostgreSQL(),
      new ByteMicrosoftSQL(),
      new ByteOracleDB(),
      new MariaDB(),
      new PostgreSQL(),
      new OracleDB(),
      new MicrosoftSQL(),
      new MongoDB(),
      new Redis(),
      new Neo4j(),
      new Cassandra()
    );
  }
}