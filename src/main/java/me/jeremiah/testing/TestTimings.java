package me.jeremiah.testing;

import com.google.gson.JsonObject;
import lombok.Getter;
import me.jeremiah.utils.TimeUtils;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestTimings {

  private final ArrayList<Timing> operationTimings = new ArrayList<>();

  public void time() {
    time(null);
  }

  public void time(@Nullable DatabaseOperation nextOperation) {
    if (!operationTimings.isEmpty() && !operationTimings.getLast().isComplete())
      operationTimings.getLast().end();
    if (nextOperation != null)
      operationTimings.add(new Timing(nextOperation).start());
  }

  public long getTotalTime() {
    if (operationTimings.isEmpty() || !operationTimings.getLast().isComplete())
      throw new IllegalStateException("Cannot get total time before test is finished");
    return operationTimings.getLast().getEnd() - operationTimings.getFirst().getStart();
  }

  public String getTimings() {
    if (operationTimings.isEmpty() || operationTimings.getLast().isComplete())
      throw new IllegalStateException("Cannot get timings before test is finished");
    return TimeUtils.formatTime(getTotalTime())
      + ", " +
      String.join(", ", operationTimings.stream()
        .map(Timing::getDuration)
        .map(TimeUtils::formatTime)
      .toArray(String[]::new)
    );
  }

  public Map<String, String> getMappings() {
    if (operationTimings.isEmpty() || !operationTimings.getLast().isComplete())
      throw new IllegalStateException("Cannot get mappings before test is finished");
    Map<String, String> mappings = new LinkedHashMap<>();

    mappings.put("Total_Time", TimeUtils.formatTime(getTotalTime()));

    Map<DatabaseOperation, Integer> counts = new LinkedHashMap<>();
    for (Timing timing : operationTimings) {
      if (counts.containsKey(timing.getOperation()))
        mappings.put(timing.getOperation().getName() + "_" + counts.get(timing.getOperation()), TimeUtils.formatTime(timing.getDuration()));
      else
        mappings.put(timing.getOperation().getName(), TimeUtils.formatTime(timing.getDuration()));
      counts.put(timing.getOperation(), counts.getOrDefault(timing.getOperation(), 0) + 1);
    }

    return mappings;
  }

  public JsonObject toJson() {
    if (operationTimings.isEmpty() || operationTimings.getLast().isComplete())
      throw new IllegalStateException("Cannot convert to JSON before test is finished");
    JsonObject json = new JsonObject();
    getMappings().forEach(json::addProperty);
    return json;
  }

  @Getter
  public static class Timing {

    private final DatabaseOperation operation;
    private long start;
    private long end;

    public Timing(DatabaseOperation operation) {
      this.operation = operation;
    }

    public Timing start() {
      System.out.println(operation.getStartMessage());
      start = System.nanoTime();
      return this;
    }

    public Timing end() {
      end = System.nanoTime();
      System.out.println(operation.getEndMessage(TimeUtils.formatTime(getDuration())));
      return this;
    }

    public boolean isComplete() {
      return start != 0 && end != 0;
    }

    public long getDuration() {
      return end - start;
    }

  }
}
