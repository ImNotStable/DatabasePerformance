package me.jeremiah.databases.nosql;

import com.google.common.primitives.Ints;
import me.jeremiah.Entry;
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
      for (Entry entry : entries) {
        pipeline.set(Ints.toByteArray(entry.getId()), entry.bytes());
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
      List<byte[]> keys = new ArrayList<>();

      // Perform the scan operation outside the pipeline
      do {
        ScanResult<byte[]> scanResult = jedis.scan(cursor.getBytes(), scanParams);
        keys.addAll(scanResult.getResult());
        cursor = scanResult.getCursor();
      } while (!cursor.equals("0"));

      // Use the pipeline to get the values
      Pipeline pipeline = jedis.pipelined();
      List<Response<byte[]>> responses = new ArrayList<>();
      for (byte[] key : keys) {
        responses.add(pipeline.get(key));
      }
      pipeline.sync();

      // Process the responses
      for (int i = 0; i < keys.size(); i++) {
        byte[] value = responses.get(i).get();
        if (value != null) {
          int id = Ints.fromByteArray(keys.get(i));
          entries.put(id, new Entry(id, value));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return entries;
  }

}