package me.jeremiah.testing;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum DatabaseOperation {
  INITIALIZATION("Initialization", "Initializing Database", "Completed %s in %s"),
  INSERTION("Insertion"),
  VERIFICATION("Verification"),
  EXISTENCE("Existence"),
  RETRIEVAL("Retrieval"),
  UPDATING("Update"),
  REMOVAL("Removal"),
  REALISM("Realism"),
  END("End", "Ending Test", "Completed Test in %s");

  @Getter
  private final String name;
  private final String startMessage;
  private final String endMessage;

  DatabaseOperation(String name) {
    this(name, "Starting %s Test", "Completed %s Test in %s");
  }

  public String getStartMessage() {
    return String.format(startMessage, name);
  }

  public String getEndMessage(String duration) {
    return String.format(endMessage, name, duration);
  }

}
