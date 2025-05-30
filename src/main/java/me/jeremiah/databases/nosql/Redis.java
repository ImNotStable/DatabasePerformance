package me.jeremiah.databases.nosql;

import me.jeremiah.Entry;
import me.jeremiah.ExceptionManager;
import me.jeremiah.databases.Database;
import org.jetbrains.annotations.NotNull;
import redis.clients.jedis.*;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Redis implements Database {

  private JedisPool jedisPool;

  @Override
  public void open() {
    JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setMaxTotal(128);
    poolConfig.setMaxIdle(128);
    poolConfig.setMinIdle(16);
    jedisPool = new JedisPool(poolConfig, "localhost", 6379, 100000);
  }

  @Override
  public void close() {
    jedisPool.close();
  }

  @Override
  public void wipe() {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.flushAll();
    } catch (Exception exception) {
      ExceptionManager.handleException(this, exception);
    }
  }

  @Override
  public void insert(@NotNull Entry @NotNull ... entries) {
    try (Jedis jedis = jedisPool.getResource()) {
      Pipeline pipeline = jedis.pipelined();
      for (Entry entry : entries) {
        String key = String.valueOf(entry.getId());
        Map<String, String> entryMap = new HashMap<>();
        entryMap.put("first_name", entry.getFirstName());
        entryMap.put("middle_initial", String.valueOf(entry.getMiddleInitial()));
        entryMap.put("last_name", entry.getLastName());
        entryMap.put("age", String.valueOf(entry.getAge()));
        entryMap.put("net_worth", String.valueOf(entry.getNetWorth()));
        pipeline.hmset(key, entryMap);
      }
      pipeline.sync();
    }
  }

  @Override
  public void update(@NotNull Entry @NotNull ... entries) {
    insert(entries);
  }

  @Override
  public void remove(@NotNull Integer @NotNull ... ids) {
    try (Jedis jedis = jedisPool.getResource()) {
      Pipeline pipeline = jedis.pipelined();
      for (int id : ids) {
        pipeline.del(String.valueOf(id));
      }
      pipeline.sync();
    }
  }

  @Override
  public boolean exists(int id) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.exists(String.valueOf(id));
    }
  }

  @Override
  public Map<Integer, Entry> select() {
    Map<Integer, Entry> entries = new HashMap<>();
    try (Jedis jedis = jedisPool.getResource()) {
      String cursor = "0";
      ScanParams scanParams = new ScanParams().match("*").count(1000);
      List<String> keys = new ArrayList<>();

      do {
        ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
        keys.addAll(scanResult.getResult());
        cursor = scanResult.getCursor();
      } while (!cursor.equals("0"));

      Pipeline pipeline = jedis.pipelined();
      List<Response<Map<String, String>>> responses = new ArrayList<>();
      for (String key : keys) {
        responses.add(pipeline.hgetAll(key));
      }
      pipeline.sync();

      for (int i = 0; i < keys.size(); i++) {
        Map<String, String> entryMap = responses.get(i).get();
        int id = Integer.parseInt(keys.get(i));
        String firstName = entryMap.get("first_name");
        char middleInitial = entryMap.get("middle_initial").charAt(0);
        String lastName = entryMap.get("last_name");
        int age = Integer.parseInt(entryMap.get("age"));
        double netWorth = Double.parseDouble(entryMap.get("net_worth"));
        entries.put(id, new Entry(id, firstName, middleInitial, lastName, age, netWorth));
      }
    }
    return entries;
  }

}