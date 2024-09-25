package me.jeremiah.testing;

import com.google.gson.JsonObject;
import lombok.Getter;
import me.jeremiah.utils.TimeUtils;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Getter
public class TestTimings {

  private final ArrayList<Timing> rawTimings = new ArrayList<>();

  public void time() {
    time(null);
  }

  public void time(@Nullable DatabaseOperation nextOperation) {
    if (!rawTimings.isEmpty() && !rawTimings.getLast().isComplete())
      rawTimings.getLast().end();
    if (nextOperation != null)
      rawTimings.add(new Timing(nextOperation).start());
  }

  public long getTotalTime() {
    if (rawTimings.isEmpty() || !rawTimings.getLast().isComplete())
      throw new IllegalStateException("Cannot get total time before test is finished");
    return rawTimings.getLast().getEnd() - rawTimings.getFirst().getStart();
  }

  public long getTotalTime(@Nullable DatabaseOperation operation) {
    if (rawTimings.isEmpty() || !rawTimings.getLast().isComplete())
      throw new IllegalStateException("Cannot get total time before test is finished");
    List<Timing> timings = rawTimings.stream()
      .filter(timing -> operation == null || timing.getOperation().equals(operation))
      .toList();
    return timings.getLast().getEnd() - timings.getFirst().getStart();
  }

  public Map<String, Long> getNumericalMappings() {
    if (rawTimings.isEmpty() || !rawTimings.getLast().isComplete())
      throw new IllegalStateException("Cannot get mappings before test is finished");
    Map<String, Long> mappings = new LinkedHashMap<>();

    mappings.put("Total_Time", getTotalTime());

    Map<DatabaseOperation, Integer> counts = new LinkedHashMap<>();
    for (Timing timing : rawTimings)
      if (rawTimings.stream().filter(timing1 -> timing1.getOperation().equals(timing.getOperation())).count() > 1)
        counts.put(timing.getOperation(), 0);

    for (Timing timing : rawTimings) {
      if (counts.containsKey(timing.getOperation())) {
        counts.computeIfPresent(timing.getOperation(), (_, count) -> count + 1);
        mappings.put(
          String.format("%s_%d", timing.getOperation().getName(), counts.get(timing.getOperation())),
          timing.getDuration()
        );
      } else
        mappings.put(timing.getOperation().getName(), timing.getDuration());
    }

    return mappings;
  }

  public Map<String, String> getFormattedMappings() {
    return getNumericalMappings()
      .entrySet()
      .stream().map(entry -> Map.entry(entry.getKey(), TimeUtils.formatTime(entry.getValue())))
      .collect(LinkedHashMap::new, (map, entry) -> map.put(entry.getKey(), entry.getValue()), LinkedHashMap::putAll);
  }

  public JsonObject toJson() {
    if (rawTimings.isEmpty() || rawTimings.getLast().isComplete())
      throw new IllegalStateException("Cannot convert to JSON before test is finished");
    JsonObject json = new JsonObject();
    getFormattedMappings().forEach(json::addProperty);
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

    public void end() {
      end = System.nanoTime();
      System.out.println(operation.getEndMessage(TimeUtils.formatTime(getDuration())));
    }

    public boolean isComplete() {
      return start != 0 && end != 0;
    }

    public long getDuration() {
      return end - start;
    }

  }
}
