package me.jeremiah.databases;

import me.jeremiah.Entry;
import me.jeremiah.ExceptionManager;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

public interface Database {

  String getName();

  void open();

  void close();

  void wipe();

  void insert(@NotNull Entry... entries);

  void update(@NotNull Entry... entry);

  default boolean exists(@NotNull Entry entry) {
    return exists(entry.getId());
  }

  boolean exists(int id);

  default void remove(@NotNull Entry... entries) {
    int[] ids = new int[entries.length];
    for (int i = 0; i < entries.length; i++)
      ids[i] = entries[i].getId();
    remove(ids);
  }

  void remove(int... ids);

  Map<Integer, Entry> select();

  default boolean verifyData(@NotNull Entry @NotNull ... entries) {
    Map<Integer, Entry> existingEntries = select();
    if (entries.length != existingEntries.size()) {
      ExceptionManager.handleException(this, new IllegalStateException("Dataset size mismatch: " + entries.length + " != " + existingEntries.size()));
      return false;
    }
    for (Entry entry : entries)
      if (!entry.equals(existingEntries.get(entry.getId())))
        return false;
    return true;
  }

  default boolean verifyData(int @NotNull ... ids) {
    for (int id : ids)
      if (!exists(id))
        return false;
    return true;
  }

}
