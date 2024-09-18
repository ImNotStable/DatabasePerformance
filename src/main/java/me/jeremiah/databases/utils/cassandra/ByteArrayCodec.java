package me.jeremiah.databases.utils.cassandra;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.primitives.Bytes;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public class ByteArrayCodec implements TypeCodec<byte[]> {

  @Override
  public @NotNull GenericType<byte[]> getJavaType() {
    return GenericType.of(byte[].class);
  }

  @Override
  public @NotNull DataType getCqlType() {
    return TypeCodecs.BLOB.getCqlType();
  }

  @Override
  public ByteBuffer encode(byte[] value, @NotNull ProtocolVersion protocolVersion) {
    return value == null ? null : ByteBuffer.wrap(value);
  }

  @Override
  public byte[] decode(ByteBuffer bytes, @NotNull ProtocolVersion protocolVersion) {
    return bytes == null ? null : bytes.array();
  }

  @Override
  public @NotNull String format(byte[] value) {
    return value == null ? "NULL" : Bytes.asList(value).toString();
  }

  @Override
  public byte[] parse(String value) {
    throw new UnsupportedOperationException("Parsing not supported for byte[]");
  }

}