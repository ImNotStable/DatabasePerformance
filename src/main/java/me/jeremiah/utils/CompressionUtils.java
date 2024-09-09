package me.jeremiah.utils;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import me.jeremiah.DatabaseSerializable;
import me.jeremiah.Entry;

import java.nio.ByteBuffer;

public class CompressionUtils {

  private static final LZ4Factory factory = LZ4Factory.fastestInstance();
  private static final LZ4Compressor compressor = factory.fastCompressor();
  private static final LZ4FastDecompressor decompressor = factory.fastDecompressor();

  public static byte[] compress(DatabaseSerializable entry) {
    byte[] data = entry.bytes();
    int maxCompressedLength = compressor.maxCompressedLength(data.length);
    byte[] compressed = new byte[maxCompressedLength + Integer.BYTES];
    ByteBuffer buffer = ByteBuffer.wrap(compressed);
    buffer.putInt(data.length);
    compressor.compress(data, 0, data.length, compressed, Integer.BYTES, maxCompressedLength);
    return compressed;
  }

  public static Entry decompress(int id, byte[] compressedData) {
    ByteBuffer buffer = ByteBuffer.wrap(compressedData);
    int originalLength = buffer.getInt();
    byte[] decompressed = new byte[originalLength];
    decompressor.decompress(compressedData, Integer.BYTES, decompressed, 0, originalLength);
    return new Entry(id, decompressed);
  }
}