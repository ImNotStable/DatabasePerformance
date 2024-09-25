package me.jeremiah.testing;

import com.google.gson.JsonObject;
import me.jeremiah.ExceptionManager;
import me.jeremiah.databases.Database;
import me.jeremiah.utils.FileUtils;
import me.jeremiah.utils.TimeUtils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

public class TestCluster {

  public static TestCluster test(Collection<Database> databases, int... entryAmounts) {
    return new TestCluster(databases, entryAmounts);
  }

  private final List<DatabaseTester> testers = new ArrayList<>();

  private TestCluster(Collection<Database> databases, int... entryAmounts) {
    databases.forEach(database -> testers.add(DatabaseTester.test(database, entryAmounts)));
  }

  public void start() {
    for (DatabaseTester databaseTester : testers) {
      synchronized (this) {
        databaseTester.start();
        try {
          this.wait(2500);
        } catch (InterruptedException exception) {
          ExceptionManager.handleException(null, exception);
        }
      }
    }
    testers.stream()
      .sorted(Comparator.comparing(tester -> tester.getTimings().values().stream().mapToLong(TestTimings::getTotalTime).sum()))
      .forEach(tester ->
        System.out.printf("%s: %s (%s) %n",
          tester.getDatabase().getName(),
          TimeUtils.formatTime(tester.getTimings().values().stream().mapToLong(TestTimings::getTotalTime).sum()),
          tester.verified())
      );
  }

  public JsonObject toJson() {
    JsonObject root = new JsonObject();
    JsonObject databaseTests = new JsonObject();

    testers.stream()
      .sorted(Comparator.comparing(tester -> tester.getTimings().values().stream().mapToLong(TestTimings::getTotalTime).sum()))
      .forEach(tester -> databaseTests.add(tester.getDatabase().getName(), tester.toJson()));

    root.add("Database_Tests", databaseTests);
    return root;
  }

  public void createLog() {
    Path logFilePath = Paths.get("P:/IntelliJProjects/DatabasePerformance/.logs/test-cluster-" + TimeUtils.getDateTime() + ".json");
    FileUtils.createFile(logFilePath);
    FileUtils.saveJsonToFile(logFilePath, toJson());
  }

}
