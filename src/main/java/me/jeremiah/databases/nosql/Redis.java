package me.jeremiah.databases.nosql;

import me.jeremiah.Entry;
import me.jeremiah.databases.Database;
import org.jetbrains.annotations.NotNull;
import redis.clients.jedis.*;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.*;

public class Redis implements Database {

  private JedisPool jedisPool;

  @Override
  public String getName() {
    return "Redis";
  }

  @Override
  public void open() {
    try {
      JedisPoolConfig poolConfig = new JedisPoolConfig();
      poolConfig.setMaxTotal(128);
      poolConfig.setMaxIdle(128);
      poolConfig.setMinIdle(16);
      poolConfig.setTestOnBorrow(true);
      poolConfig.setTestOnReturn(true);
      poolConfig.setTestWhileIdle(true);
      jedisPool = new JedisPool(poolConfig, "localhost", 6379);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() {
      try {
        jedisPool.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
  }

  @Override
  public void wipe() {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.flushAll();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void insert(@NotNull Entry... entries) {
    try (Jedis jedis = jedisPool.getResource()) {
      Pipeline pipeline = jedis.pipelined();
      Map<String, String> rawEntryData = new HashMap<>();
      for (Entry entry : entries) {
        rawEntryData.put("firstName", entry.getFirstName());
        rawEntryData.put("middleInitial", String.valueOf(entry.getMiddleInitial()));
        rawEntryData.put("lastName", entry.getLastName());
        pipeline.hset(String.valueOf(entry.getId()), rawEntryData);
        rawEntryData.clear();
      }
      pipeline.sync();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void update(@NotNull Entry... entries) {
    insert(entries);
  }

  @Override
  public boolean exists(int id) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.exists(String.valueOf(id));
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }

  @Override
  public void remove(int... ids) {
    try (Jedis jedis = jedisPool.getResource()) {
      Pipeline pipeline = jedis.pipelined();
      String[] rawIds = new String[ids.length];
      for (int i = 0; i < ids.length; i++)
        rawIds[i] = String.valueOf(ids[i]);
      pipeline.del(rawIds);
      pipeline.sync();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public Map<Integer, Entry> select() {
    Map<Integer, Entry> entries = new HashMap<>();
    try (Jedis jedis = jedisPool.getResource()) {
      String cursor = "0";
      ScanParams scanParams = new ScanParams().match("*").count(100);

      do {
        ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
        List<String> keys = scanResult.getResult();
        cursor = scanResult.getCursor();

        Pipeline pipeline = jedis.pipelined();
        List<Response<Map<String, String>>> responses = new ArrayList<>();
        for (String key : keys) {
          responses.add(pipeline.hgetAll(key));
        }
        pipeline.sync();

        for (int i = 0; i < keys.size(); i++) {
          Map<String, String> entryMap = responses.get(i).get();
          int id = Integer.parseInt(keys.get(i));
          entries.put(id, new Entry(id, entryMap.get("firstName"), entryMap.get("middleInitial").charAt(0), entryMap.get("lastName")));
        }
      } while (!cursor.equals("0"));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return entries;
  }

}