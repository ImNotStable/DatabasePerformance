package me.jeremiah.testing;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import lombok.Getter;
import me.jeremiah.Entry;
import me.jeremiah.databases.Database;
import me.jeremiah.utils.TimeUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class DatabaseTester {

  public static Collection<DatabaseTester> testCluster(Collection<Database> databases, int... entryAmount) {
    Collection<DatabaseTester> testers = new ArrayList<>(databases.size());
    for (Database database : databases) testers.add(new DatabaseTester(database, entryAmount));
    for (DatabaseTester tester : testers) tester.start();

    testers.stream()
      .sorted(Comparator.comparing(tester -> tester.timings.values().stream().mapToLong(TestTimings::getTotalTime).sum()))
      .forEach(tester -> System.out.println(tester.database.getName() + ": " + TimeUtils.formatTime(tester.timings.values().stream().mapToLong(TestTimings::getTotalTime).sum())));

    Path logFilePath = Paths.get("P:/IntelliJProjects/DatabasePerformance/logs/test-cluster-" + TimeUtils.getDateTime() + ".json");
    try {
      Files.createDirectories(logFilePath.getParent());
      Files.createFile(logFilePath);
      System.out.println("Successfully created log file");
    } catch (IOException exception) {
      exception.printStackTrace();
    }

    JsonObject root = new JsonObject();
    testers.stream()
      .sorted(Comparator.comparing(tester -> tester.timings.values().stream().mapToLong(TestTimings::getTotalTime).sum()))
      .forEach(tester -> root.add(tester.getDatabase().getName(), tester.toJson()));

    testers.forEach(tester -> root.add(tester.getDatabase().getName(), tester.toJson()));
    try (FileWriter writer = new FileWriter(new File(logFilePath.toUri()))) {
      writer.write(new GsonBuilder().setPrettyPrinting().create().toJson(root));
    } catch (IOException exception) {
      exception.printStackTrace();
    }
    return testers;
  }

  public static DatabaseTester test(Database database, int... entryAmount) {
    DatabaseTester tester = new DatabaseTester(database, entryAmount);
    tester.start();
    tester.createLog();
    return tester;
  }

  @Getter
  private final Database database;

  private final int[] entryAmounts;
  private final Entry[] entries;

  private final Map<Integer, TestTimings> timings = new LinkedHashMap<>();
  private TestTimings currentTimings;

  private final boolean[] verificationResults;

  private DatabaseTester(Database database, int... entryAmounts) {
    this.database = database;
    this.entryAmounts = entryAmounts;
    int maxEntryAmount = Arrays.stream(entryAmounts).max().orElse(0);
    this.entries = new Entry[maxEntryAmount];
    this.verificationResults = new boolean[entryAmounts.length];
  }

  private void generateEntries() {
    Arrays.parallelSetAll(entries, Entry::new);
  }

  public void start() {
    System.out.println("Starting Database Test for " + database.getName() + " [" + Arrays.stream(entryAmounts).mapToObj(String::valueOf).collect(Collectors.joining(", ")) + "]");
    generateEntries();

    System.out.println("Initializing " + database.getName());
    database.open();
    int index = 0;
    for (int currentEntryAmount : entryAmounts) {
      System.out.println("Testing " + database.getName() + " for " + currentEntryAmount + " Entries");
      database.wipe();
      currentTimings = new TestTimings();
      runInsertionTest(currentEntryAmount);
      runVerificationTest(index);
      runRetrievalTest();
      runUpdatingTest((int) (currentEntryAmount * 0.1));
      runRemovalTest((int) (currentEntryAmount * 0.1));
      runRetrievalTest();
      currentTimings.time();
      timings.put(currentEntryAmount, currentTimings);
      index++;
    }
    database.close();
  }

  private void open() {
    currentTimings.time(DatabaseOperation.INITIALIZATION);
    database.open();
  }

  private void runInsertionTest(int entryAmount) {
    currentTimings.time(DatabaseOperation.INSERTION);
    Entry[] relevantEntries = Arrays.copyOf(entries, entryAmount);
    database.insert(relevantEntries);
  }

  private void runVerificationTest(int index) {
    currentTimings.time(DatabaseOperation.VERIFICATION);
    verificationResults[index] = database.verifyData(Arrays.copyOf(entries, entryAmounts[index]));
  }

  private void runUpdatingTest(int entryAmount) {
    currentTimings.time(DatabaseOperation.UPDATING);
    Entry[] relevantEntries = Arrays.copyOf(entries, entryAmount);
    database.update(relevantEntries);
  }

  private void runRemovalTest(int entryAmount) {
    currentTimings.time(DatabaseOperation.REMOVAL);
    Entry[] relevantEntries = Arrays.copyOf(entries, entryAmount);
    database.remove(relevantEntries);
  }

  private void runExistenceTest() {
    currentTimings.time(DatabaseOperation.EXISTENCE);
    for (Entry entry : entries)
      database.exists(entry);
  }

  private void runRetrievalTest() {
    currentTimings.time(DatabaseOperation.RETRIEVAL);
    database.select();
  }

  private void runRealismTest() {
    currentTimings.time(DatabaseOperation.REALISM);
  }

  private void close() {
    currentTimings.time(DatabaseOperation.END);
    database.close();
    currentTimings.time();
  }

  public Map<String, Map<String, String>> getTimeMappings() {
    return timings.entrySet().stream()
      .collect(Collectors.toMap(
        entry -> entry.getKey().toString() + " Entries",
        entry -> entry.getValue().getMappings().entrySet().stream()
          .collect(Collectors.toMap(
            Map.Entry::getKey,
            Map.Entry::getValue,
            (existing, replacement) -> existing,
            LinkedHashMap::new
          )),
        (existing, replacement) -> existing,
        LinkedHashMap::new
      ));
  }

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    json.addProperty("Entries", Arrays.stream(entryAmounts).mapToObj(String::valueOf).collect(Collectors.joining(", ")));
    json.addProperty("Total_Test_Time",
      TimeUtils.formatTime(timings.values().stream().mapToLong(TestTimings::getTotalTime).sum()));
    json.addProperty("Verification_Results", Arrays.toString(verificationResults));
    getTimeMappings().values().stream()
      .flatMap(mappings -> mappings.entrySet().stream())
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1 + ", " + v2, LinkedHashMap::new))
      .forEach(json::addProperty);
    return json;
  }

  public void createLog() {
    Path logFilePath = Paths.get("P:/IntelliJProjects/DatabasePerformance/logs/" + database.getName() + "-" + TimeUtils.getDateTime() + ".json");
    try {
      Files.createDirectories(logFilePath.getParent());
      Files.createFile(logFilePath);
      System.out.println("Successfully created log file");
    } catch (IOException exception) {
      exception.printStackTrace();
    }
    try (FileWriter writer = new FileWriter(new File(logFilePath.toUri()))) {
      writer.write(new GsonBuilder().setPrettyPrinting().create().toJson(toJson()));
    } catch (IOException exception) {
      exception.printStackTrace();
    }
  }

}
