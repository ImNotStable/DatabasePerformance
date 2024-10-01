package me.jeremiah.databases;

import me.jeremiah.Entry;
import me.jeremiah.ExceptionManager;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Map;

public interface Database {

  default String getName() {
    return getClass().getSimpleName();
  }

  void open();

  void close();

  void wipe();

  void insert(@NotNull Entry @NotNull ... entries);

  void update(@NotNull Entry @NotNull ... entry);

  default void remove(@NotNull Entry @NotNull ... entries) {
    Integer[] ids = new Integer[entries.length];
    Arrays.setAll(ids, i -> entries[i].getId());
    remove(ids);
  }

  void remove(@NotNull Integer @NotNull ... ids);

  default boolean exists(@NotNull Entry entry) {
    return exists(entry.getId());
  }

  boolean exists(int id);

  Map<Integer, Entry> select();

  default boolean verifyData(@NotNull Entry @NotNull ... entries) {
    Map<Integer, Entry> existingEntries = select();
    if (entries.length != existingEntries.size()) {
      ExceptionManager.handleException(this, new IllegalStateException(String.format("Dataset size mismatch: %s != %s", entries.length, existingEntries.size())));
      return false;
    }
    return Arrays.stream(entries).allMatch(entry -> entry.equals(existingEntries.get(entry.getId())));
  }

}
