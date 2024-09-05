package me.jeremiah.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TimeUtils {

  public static String getDateTime() {
    return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy.MM.dd HH-mm-ss"));
  }

  public static String formatTime(long duration) {
    if (duration < 1_000_000) return duration + "ns";
    else if (duration < 1_000_000_000) return (duration / 1_000_000.0) + "ms";
    else if (duration < 60_000_000_000_000L) return (duration / 1_000_000_000.0) + "s";
    else return (duration / 60_000_000_000_000.0) + "m";
  }

}
