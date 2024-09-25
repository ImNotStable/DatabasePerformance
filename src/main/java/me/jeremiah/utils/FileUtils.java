package me.jeremiah.utils;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import me.jeremiah.ExceptionManager;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtils {

  public static boolean createFile(String rawPath) {
    return createFile(Paths.get(rawPath));
  }

  public static boolean createFile(File file) {
    return createFile(file.toPath());
  }

  public static boolean createFile(Path path) {
    try {
      Files.createDirectories(path.getParent());
      Files.createFile(path);
    } catch (IOException exception) {
      ExceptionManager.handleException(null, exception);
    }
    return path.toFile().exists();
  }

  public static void saveJsonToFile(String rawPath, JsonObject json) {
    saveJsonToFile(Paths.get(rawPath), json);
  }

  public static void saveJsonToFile(Path path, JsonObject json) {
    saveJsonToFile(path.toFile(), json);
  }

  public static void saveJsonToFile(File file, JsonObject json) {
    try (FileWriter fileWriter = new FileWriter(file)) {
      saveJsonToFile(fileWriter, json);
    } catch (IOException exception) {
      ExceptionManager.handleException(null, exception);
    }
  }

  public static void saveJsonToFile(FileWriter fileWriter, JsonObject json) throws IOException {
    fileWriter.write(new GsonBuilder().setPrettyPrinting().create().toJson(json));
  }

}
