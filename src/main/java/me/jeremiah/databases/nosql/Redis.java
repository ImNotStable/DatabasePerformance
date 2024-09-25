package me.jeremiah.databases.nosql;

import com.google.common.primitives.Ints;
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
      jedisPool = new JedisPool(poolConfig, "localhost", 6379, 100000);
    } catch (Exception exception) {
      ExceptionManager.handleException(this, exception);
    }
  }

  @Override
  public void close() {
    try {
      jedisPool.close();
    } catch (Exception exception) {
      ExceptionManager.handleException(this, exception);
    }
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
      for (Entry entry : entries)
        pipeline.set(Ints.toByteArray(entry.getId()), entry.bytes());
      pipeline.sync();
    } catch (Exception exception) {
      ExceptionManager.handleException(this, exception);
    }
  }

  @Override
  public void update(@NotNull Entry @NotNull ... entries) {
    insert(entries);
  }

  @Override
  public boolean exists(int id) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.exists(Ints.toByteArray(id));
    } catch (Exception exception) {
      ExceptionManager.handleException(this, exception);
      return false;
    }
  }

  @Override
  public void remove(int... ids) {
    try (Jedis jedis = jedisPool.getResource()) {
      Pipeline pipeline = jedis.pipelined();
      byte[][] rawIds = new byte[ids.length][];
      for (int i = 0; i < ids.length; i++)
        rawIds[i] = Ints.toByteArray(ids[i]);
      pipeline.del(rawIds);
      pipeline.sync();
    } catch (Exception exception) {
      ExceptionManager.handleException(this, exception);
    }
  }

  @Override
  public Map<Integer, Entry> select() {
    Map<Integer, Entry> entries = new HashMap<>();
    try (Jedis jedis = jedisPool.getResource()) {
      String cursor = "0";
      ScanParams scanParams = new ScanParams().match("*").count(1000);
      List<byte[]> keys = new ArrayList<>();

      do {
        ScanResult<byte[]> scanResult = jedis.scan(cursor.getBytes(), scanParams);
        keys.addAll(scanResult.getResult());
        cursor = scanResult.getCursor();
      } while (!cursor.equals("0"));

      Pipeline pipeline = jedis.pipelined();
      List<Response<byte[]>> responses = new ArrayList<>();
      for (byte[] key : keys)
        responses.add(pipeline.get(key));
      pipeline.sync();

      for (int i = 0; i < keys.size(); i++) {
        int id = Ints.fromByteArray(keys.get(i));
        byte[] value = responses.get(i).get();
        entries.put(id, new Entry(id, value));
      }
    } catch (Exception exception) {
      ExceptionManager.handleException(this, exception);
    }
    return entries;
  }

}