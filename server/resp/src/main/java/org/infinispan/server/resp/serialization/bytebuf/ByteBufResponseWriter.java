package org.infinispan.server.resp.serialization.bytebuf;

import static org.infinispan.server.resp.serialization.RespConstants.ARRAY;
import static org.infinispan.server.resp.serialization.RespConstants.CRLF;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.infinispan.server.resp.ByteBufPool;
import org.infinispan.server.resp.RespVersion;
import org.infinispan.server.resp.serialization.JavaObjectSerializer;
import org.infinispan.server.resp.serialization.NestedResponseSerializer;
import org.infinispan.server.resp.serialization.Resp3Type;
import org.infinispan.server.resp.serialization.RespConstants;
import org.infinispan.server.resp.serialization.ResponseSerializer;
import org.infinispan.server.resp.serialization.ResponseWriter;
import org.infinispan.server.resp.serialization.SerializationHint;

import io.netty.buffer.ByteBuf;

public final class ByteBufResponseWriter implements ResponseWriter {
   private final ByteBufPool alloc;
   private RespVersion version = RespVersion.RESP3;

   public ByteBufResponseWriter(ByteBufPool alloc) {
      this.alloc = alloc;
   }

   @Override
   public RespVersion version() {
      return version;
   }

   @Override
   public void version(RespVersion version) {
      this.version = version;
   }

   @Override
   public boolean isInternal() {
      return false;
   }

   @Override
   public void nulls() {
      ByteBufPrimitiveSerializer.NullSerializer.INSTANCE.accept(null, alloc);
   }

   @Override
   public void ok() {
      simpleString(RespConstants.OK);
   }

   @Override
   public void queued(Object ignore) {
      simpleString(RespConstants.QUEUED_REPLY);
   }

   @Override
   public void simpleString(CharSequence value) {
      serialize(value, ByteBufPrimitiveSerializer.SimpleStringSerializer.INSTANCE);
   }

   @Override
   public void string(CharSequence value) {
      serialize(value, ByteBufPrimitiveSerializer.BulkStringSerializer2.INSTANCE);
   }

   @Override
   public void string(byte[] value) {
      serialize(value, ByteBufPrimitiveSerializer.BulkStringSerializer.INSTANCE);
   }

   @Override
   public void integers(Number value) {
      serialize(value, ByteBufPrimitiveSerializer.IntegerSerializer.INSTANCE);
   }

   @Override
   public void doubles(Number value) {
      serialize(value, ByteBufDoubleSerializer.INSTANCE);
   }

   @Override
   public void booleans(boolean value) {
      serialize(value, ByteBufPrimitiveSerializer.BooleanSerializer.INSTANCE);
   }

   @Override
   public void arrayEmpty() {
      write(Collections.emptyList(), (ignore, writer) -> {
         writer.arrayStart(0);
         writer.arrayEnd();
      });
   }

   @SuppressWarnings("unchecked")
   @Override
   public <T> void array(Collection<T> collection, JavaObjectSerializer<T> serializer) {
      serialize(collection, ByteBufCollectionSerializer.ArraySerializer.INSTANCE, (o, b) -> serializer.accept((T) o, b));
   }

   @Override
   public void array(Collection<?> collection, Resp3Type contentType) {
      serialize(collection, ByteBufCollectionSerializer.ArraySerializer.INSTANCE, contentType);
   }

   @Override
   public void emptySet() {
      write(Collections.emptySet(), (ignore, writer) -> writer.writeNumericPrefix(RespConstants.SET, 0));
   }

   @Override
   public void set(Set<?> set, Resp3Type contentType) {
      serialize(set, ByteBufCollectionSerializer.SetSerializer.INSTANCE, contentType);
   }

   @Override
   public <T> void set(Set<?> set, JavaObjectSerializer<T> serializer) {
      serialize(set, ByteBufCollectionSerializer.SetSerializer.INSTANCE, (o, b) -> serializer.accept((T) o, b));
   }

   @Override
   public void map(Map<?, ?> value) {
      serialize(value, ByteBufMapSerializer.INSTANCE);
   }

   @Override
   public void map(Map<?, ?> value, Resp3Type contentType) {
      serialize(value, ByteBufMapSerializer.INSTANCE, new SerializationHint.KeyValueHint(contentType, contentType));
   }

   @Override
   public void map(Map<?, ?> value, Resp3Type keyType, Resp3Type valueType) {
      serialize(value, ByteBufMapSerializer.INSTANCE, new SerializationHint.KeyValueHint(keyType, valueType));
   }

   @Override
   public void map(Map<?, ?> map, SerializationHint.KeyValueHint keyValueHint) {
      serialize(map, ByteBufMapSerializer.INSTANCE, keyValueHint);
   }

   @Override
   public void error(CharSequence value) {
      serialize(value, ByteBufPrimitiveSerializer.SimpleErrorSerializer.INSTANCE);
   }

   @Override
   public void error(Throwable t) {
      serialize(t, ByteBufThrowableSerializer.INSTANCE);
   }

   @Override
   public <T> void write(T object, JavaObjectSerializer<T> serializer) {
      if (object == null) {
         nulls();
      } else {
         serializer.accept(object, this);
      }
   }

   @Override
   public <T> void write(JavaObjectSerializer<T> serializer) {
      if (serializer == null) {
         nulls();
      } else {
         serializer.accept(null, this);
      }
   }

   @Override
   public void serialize(Object object) {
      ByteBufSerializerRegistry.serialize(object, alloc);
   }

   @Override
   public void writeNumericPrefix(byte symbol, long number) {
      int decimalWidth = ByteBufferUtils.stringSize(number);
      int size = 1 + decimalWidth + 2;
      ByteBuf buffer = alloc.acquire(size);
      buffer.writeByte(symbol);
      ByteBufferUtils.setIntChars(number, decimalWidth, buffer);
      buffer.writeBytes(CRLF);
   }

   @Override
   public void arrayStart(int size) {
      writeNumericPrefix(ARRAY, size);
   }

   @Override
   public void arrayEnd() {
      // NO-OP
   }

   @Override
   public void arrayNext() {
      // NO-OP
   }

   private void serialize(Object object, ResponseSerializer<?, ByteBufPool> candidate) {
      ByteBufSerializerRegistry.serialize(object, alloc, candidate);
   }

   private <H extends SerializationHint> void serialize(Object object,
                                                        NestedResponseSerializer<?, ByteBufPool, H> candidate,
                                                        H hint) {
      ByteBufSerializerRegistry.serialize(object, alloc, candidate, hint);
   }
}
