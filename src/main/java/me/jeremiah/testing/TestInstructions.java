package me.jeremiah.testing;

import com.google.common.base.Preconditions;
import lombok.Getter;
import me.jeremiah.Entry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.BiConsumer;

public class TestInstructions implements BiConsumer<DatabaseTester, Object[]> {

  public static final TestInstructions INSERTION_TEST = new TestInstructions("Insertion", (tester, args) -> {
    Preconditions.checkArgument(args.length > 0, "No data provided for insertion");
    Preconditions.checkArgument(args[0] instanceof Integer, "Invalid data type provided for insertion");
    Integer entryAmount = (Integer) args[0];
    Entry[] entries = Arrays.copyOf(tester.getEntries(), entryAmount);
    tester.getDatabase().insert(entries);
  });
  public static final TestInstructions VERIFICATION_TEST = new TestInstructions("Verification", (tester, args) -> {
    Preconditions.checkArgument(args.length > 1, "Not enough data provided for verification");
    Preconditions.checkArgument(args[0] instanceof Integer && args[1] instanceof Integer, "Invalid data type provided for verification");
    Integer from = (Integer) args[0];
    Integer to = (Integer) args[1];
    Entry[] entries = Arrays.copyOfRange(tester.getEntries(), from, to);
    if (tester.getVerificationResults().size() <= tester.getVerificationIndex())
      tester.getVerificationResults().add(tester.getVerificationIndex(), new ArrayList<>());
    tester.getVerificationResults().get(tester.getVerificationIndex()).add(tester.getDatabase().verifyData(entries));
    int newVerificationIndex = tester.getVerificationIndex() + 1;
    tester.setVerificationIndex(newVerificationIndex);
  });
  public static final TestInstructions RETRIEVAL_TEST = new TestInstructions("Retrieval", (tester, args) -> {
    tester.getDatabase().select();
  });
  public static final TestInstructions UPDATE_TEST = new TestInstructions("Update", (tester, args) -> {
    Preconditions.checkArgument(args.length > 0, "No data provided for verification");
    Preconditions.checkArgument(args[0] instanceof Integer, "Invalid data type provided for verification");
    Integer entryAmount = (Integer) args[0];
    Entry[] entries = Arrays.copyOf(tester.getEntries(), entryAmount);
    tester.getDatabase().update(entries);
  });
  public static final TestInstructions REMOVAL_TEST = new TestInstructions("Removal", (tester, args) -> {
    Preconditions.checkArgument(args.length > 0, "No data provided for verification");
    Preconditions.checkArgument(args[0] instanceof Integer, "Invalid data type provided for verification");
    Integer entryAmount = (Integer) args[0];
    Entry[] entries = Arrays.copyOf(tester.getEntries(), entryAmount);
    tester.getDatabase().remove(entries);
  });

  @Getter
  private final String name;
  private final String startMessage;
  private final String endMessage;
  private final BiConsumer<DatabaseTester, Object[]> instruction;

  private TestInstructions(String name, String startMessage, String endMessage, BiConsumer<DatabaseTester, Object[]> instruction) {
    this.name = name;
    this.startMessage = startMessage;
    this.endMessage = endMessage;
    this.instruction = instruction;
  }

  private TestInstructions(String name, BiConsumer<DatabaseTester, Object[]> instruction) {
    this(name, "Starting %s Test", "Completed %s Test in %s", instruction);
  }

  public String getStartMessage() {
    return String.format(startMessage, name);
  }

  public String getEndMessage(String duration) {
    return String.format(endMessage, name, duration);
  }

  public void accept(DatabaseTester tester, Object... args) {
    tester.getCurrentTimings().time(this);
    instruction.accept(tester, args);
  }

}
