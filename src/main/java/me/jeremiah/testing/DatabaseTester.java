package me.jeremiah.testing;

import com.google.gson.JsonObject;
import lombok.Getter;
import me.jeremiah.Entry;
import me.jeremiah.Main;
import me.jeremiah.databases.Database;
import me.jeremiah.utils.FileUtils;
import me.jeremiah.utils.TimeUtils;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

public class DatabaseTester {

  public static DatabaseTester test(Database database, int... entryAmount) {
    return new DatabaseTester(database, entryAmount);
  }

  @Getter
  private final Database database;

  private final int[] entryAmounts;
  private final Entry[] entries;

  @Getter
  private final Map<Integer, TestTimings> timings = new LinkedHashMap<>();
  private TestTimings currentTimings;

  private final List<List<Boolean>> verificationResults;

  private int verificationIndex = -1;

  private DatabaseTester(Database database, int... entryAmounts) {
    this.database = database;
    this.entryAmounts = entryAmounts;
    int maxEntryAmount = Arrays.stream(entryAmounts).max().orElse(0);
    this.entries = new Entry[maxEntryAmount];
    this.verificationResults = new ArrayList<>();
  }

  private void generateEntries() {
    Arrays.parallelSetAll(entries, Entry::new);
  }

  public void start() {
    System.out.printf("Starting Database Test for %s [%s]%n",
      database.getName(),
      Arrays.stream(entryAmounts).mapToObj(String::valueOf).collect(Collectors.joining(", "))
    );

    generateEntries();

    System.out.printf("Initializing %s%n", database.getName());
    database.open();
    for (int currentEntryAmount : entryAmounts) {
      System.out.printf("Testing %s for %d Entries%n", database.getName(), currentEntryAmount);

      verificationIndex = 0;
      int tenPercent = (int) (currentEntryAmount * 0.1);
      currentTimings = new TestTimings();

      database.wipe();
      runInsertionTest(currentEntryAmount);
      runVerificationTest(0, currentEntryAmount);
      runRetrievalTest();
      runUpdatingTest(tenPercent);
      runVerificationTest(0, currentEntryAmount);
      runRemovalTest(tenPercent);
      runVerificationTest(tenPercent, currentEntryAmount);
      runRetrievalTest();
      currentTimings.time();
      timings.put(currentEntryAmount, currentTimings);
    }
    database.close();
  }

  private void runInsertionTest(int entryAmount) {
    currentTimings.time(DatabaseOperation.INSERTION);
    database.insert(Arrays.copyOf(entries, entryAmount));
  }

  private void runUpdatingTest(int entryAmount) {
    currentTimings.time(DatabaseOperation.UPDATING);
    database.update(Arrays.copyOf(entries, entryAmount));
  }

  private void runRemovalTest(int entryAmount) {
    currentTimings.time(DatabaseOperation.REMOVAL);
    database.remove(Arrays.copyOf(entries, entryAmount));
  }

  private void runRetrievalTest() {
    currentTimings.time(DatabaseOperation.RETRIEVAL);
    database.select();
  }

  private void runVerificationTest(int from, int to) {
    currentTimings.time(DatabaseOperation.VERIFICATION);
    if (verificationResults.size() <= verificationIndex)
      verificationResults.add(verificationIndex, new ArrayList<>());
    verificationResults.get(verificationIndex).add(database.verifyData(Arrays.copyOfRange(entries, from, to)));
    verificationIndex++;
  }

  public Map<String, Map<String, String>> getTimeMappings() {
    return timings.entrySet().stream()
      .collect(Collectors.toMap(
        entry -> entry.getKey() + " Entries",
        entry -> entry.getValue().getFormattedMappings().entrySet().stream()
          .collect(Collectors.toMap(
            Map.Entry::getKey,
            Map.Entry::getValue,
            (existing, _) -> existing,
            LinkedHashMap::new
          )),
        (existing, _) -> existing,
        LinkedHashMap::new
      ));
  }

  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    json.addProperty("Entries", Arrays.stream(entryAmounts).mapToObj(String::valueOf).collect(Collectors.joining(", ")));
    json.addProperty("Total_Test_Time",
      TimeUtils.formatTime(timings.values().stream().mapToLong(TestTimings::getTotalTime).sum()));
    for (int i = 0; i < verificationResults.size(); i++)
      json.addProperty("Verification_Results_" + (i + 1), verificationResults.get(i).stream().map(result -> result ? "Passed" : "Failed").collect(Collectors.joining(", ")));
    getTimeMappings().values().stream()
      .flatMap(mappings -> mappings.entrySet().stream())
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1 + ", " + v2, LinkedHashMap::new))
      .forEach(json::addProperty);
    return json;
  }

  public void createLog() {
    File logFile = new File(Main.getLogDir(), String.format("%s-%s.json", database.getName(), TimeUtils.getDateTime()));
    FileUtils.createFile(logFile);
    FileUtils.saveJsonToFile(logFile, toJson());
  }

  public boolean verified() {
    for (List<Boolean> verificationResults : verificationResults)
      for (boolean result : verificationResults)
        if (!result) return false;
    return true;
  }

}
