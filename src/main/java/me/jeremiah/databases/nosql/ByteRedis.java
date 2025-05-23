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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ByteRedis implements Database {

  private JedisPool jedisPool;

  @Override
  public String getName() {
    return "Byte-Redis";
  }

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
      for (Entry entry : entries)
        pipeline.set(Ints.toByteArray(entry.getId()), entry.bytes());
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
      byte[][] rawIds = new byte[ids.length][];
      Arrays.setAll(rawIds, i -> Ints.toByteArray(ids[i]));
      pipeline.del(rawIds);
      pipeline.sync();
    }
  }

  @Override
  public boolean exists(int id) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.exists(Ints.toByteArray(id));
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
    }
    return entries;
  }

}