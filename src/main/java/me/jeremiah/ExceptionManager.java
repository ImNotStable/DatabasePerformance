package me.jeremiah;

import me.jeremiah.databases.Database;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

public class ExceptionManager {

  private static final List<LoggedException> loggedExceptions = new ArrayList<>();

  public static void handleException(Database database, Exception exception) {
    LoggedException loggedException = new LoggedException(database, exception);
    printException(loggedException);
    loggedExceptions.add(loggedException);
  }

  private static void printException(LoggedException loggedException) {
    String exceptionOrigin = loggedException.database() != null ? loggedException.database().getName() : "Java";
    System.out.printf("===== Caught Exception for %s =====%n", exceptionOrigin);
    System.out.printf("Message: %s%n", loggedException.exception().getMessage());
    loggedException.exception().printStackTrace(System.out);
    System.out.printf("%n==========================%n%n");
  }

  public static List<LoggedException> collectLoggedExceptions() {
    return new ArrayList<>(loggedExceptions);
  }

  public static void clearLoggedExceptions() {
    loggedExceptions.clear();
  }

  public record LoggedException(@Nullable Database database, @NotNull Exception exception) {
  }

}
