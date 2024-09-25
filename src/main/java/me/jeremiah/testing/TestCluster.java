package me.jeremiah.testing;

import com.google.gson.JsonObject;
import me.jeremiah.Main;
import me.jeremiah.databases.Database;
import me.jeremiah.utils.FileUtils;
import me.jeremiah.utils.TimeUtils;

import java.io.File;
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
        databaseTester.start();
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
    File logFile = new File(Main.getLogDir(), String.format("test-cluster-%s.json", TimeUtils.getDateTime()));
    FileUtils.createFile(logFile);
    FileUtils.saveJsonToFile(logFile, toJson());
  }

}
