package me.jeremiah;

import me.jeremiah.databases.Database;
import me.jeremiah.databases.nosql.Cassandra;
import me.jeremiah.databases.nosql.MongoDB;
import me.jeremiah.databases.nosql.Neo4j;
import me.jeremiah.databases.nosql.Redis;
import me.jeremiah.databases.sql.*;
import me.jeremiah.testing.TestCluster;

import java.io.File;
import java.util.List;

public final class Main {

  private static final File LOG_DIRECTORY = new File("P:/IntelliJProjects/DatabasePerformance/.logs/");

  public static void main(String[] args) {

    TestCluster cluster = TestCluster.test(getDatabaseStack(), 1_000, 10_000);
    cluster.start();
    cluster.createLog();

    int exceptions = ExceptionManager.collectLoggedExceptions().size();
    if (exceptions > 0)
      System.out.printf("Handled Exceptions: %d%n", exceptions);
  }

  public static List<Database> getDatabaseStack() {
    return List.of(
      new SQLite(),
      new ByteSQLite(),
      new H2(),
      new ByteH2(),
      new MySQL(),
      new ByteMySQL(),
      new MariaDB(),
      new ByteMariaDB(),
      new PostgreSQL(),
      new BytePostgreSQL(),
      new MicrosoftSQL(),
      new ByteMicrosoftSQL(),
      new OracleDB(),
      new ByteOracleDB(),
      new MongoDB(),
      new Redis(),
      new Neo4j(),
      new Cassandra()
    );
  }

  public static File getLogDir() {
    if (!LOG_DIRECTORY.exists())
      LOG_DIRECTORY.mkdirs();
    return LOG_DIRECTORY;
  }

}