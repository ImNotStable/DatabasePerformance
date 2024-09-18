package me.jeremiah;

import me.jeremiah.databases.Database;

import java.util.ArrayList;
import java.util.List;

public class ExceptionManager {

  private static final List<LoggedException> loggedExceptions = new ArrayList<>();

  public static void handleException(Database database, Exception exception) {
    printException(database.getName(), exception);
    loggedExceptions.add(new LoggedException(database, exception));
  }

  private static void printException(String databaseName, Exception exception) {
    System.out.printf("===== Caught Exception for %s =====%n", databaseName);
    System.out.printf("Message: %s%n", exception.getMessage());
    exception.printStackTrace(System.out);
    System.out.println("\n==========================\n");
  }

  public static List<LoggedException> collectLoggedExceptions() {
    return new ArrayList<>(loggedExceptions);
  }

  public static void clearLoggedExceptions() {
    loggedExceptions.clear();
  }


  public record LoggedException(Database database, Exception exception) {}

}
