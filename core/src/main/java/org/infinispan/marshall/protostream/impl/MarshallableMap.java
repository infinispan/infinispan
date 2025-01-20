package org.infinispan.marshall.protostream.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.ProtobufTagMarshaller;
import org.infinispan.protostream.TagReader;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.protostream.annotations.impl.GeneratedMarshallerBase;
import org.infinispan.protostream.descriptors.WireType;

/**
 * A wrapper for Maps of user objects whose key/value type is unknown until runtime.
 *
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MARSHALLABLE_MAP)
public class MarshallableMap<K, V> {

   final Map<K, V> map;

   /**
    * @param map the {@link Map} to be wrapped.
    * @return a new {@link MarshallableMap} instance containing the passed object if the object is not null,
    * otherwise null.
    */
   public static <K, V> MarshallableMap<K, V> create(Map<K, V> map) {
      return map == null ? null : new MarshallableMap<>(map);
   }

   /**
    * @param wrapper the {@link MarshallableMap} instance to unwrap.
    * @return the wrapped {@link Map} or null if the provided wrapper does not exist.
    */
   public static <K, V> Map<K, V> unwrap(MarshallableMap<K, V> wrapper) {
      return wrapper == null ? null : wrapper.get();
   }

   @ProtoFactory
   MarshallableMap(List<byte[]> keys, List<byte[]> values) {
      // no-op never actually used, as we override the default marshaller
      throw illegalState();
   }

   private MarshallableMap(Map<K, V> map) {
      this.map = map;
   }

   @ProtoField(1)
   List<byte[]> getKeys() {
      throw illegalState();
   }

   @ProtoField(2)
   List<byte[]> getValues() {
      throw illegalState();
   }

   public Map<K, V> get() {
      return map;
   }

   private IllegalStateException illegalState() {
      return new IllegalStateException(this.getClass().getSimpleName() + " marshaller not overridden in SerializationContext");
   }

   public static class Marshaller extends GeneratedMarshallerBase implements ProtobufTagMarshaller<MarshallableMap<?, ?>> {
      private final String typeName;
      private final org.infinispan.commons.marshall.Marshaller marshaller;

      public Marshaller(String typeName, org.infinispan.commons.marshall.Marshaller marshaller) {
         this.typeName = typeName;
         this.marshaller = marshaller;
      }

      @Override
      public String getTypeName() {
         return typeName;
      }

      @Override
      public MarshallableMap<?, ?> read(ReadContext ctx) throws IOException {
         final org.infinispan.protostream.TagReader in = ctx.getReader();

         ArrayList<Object> keys = new ArrayList<>();
         ArrayList<Object> values = new ArrayList<>();
         boolean done = false;
         while (!done) {
            final int tag = in.readTag();
            switch (tag) {
               case 0:
                  done = true;
                  break;
               case 1 << 3 | WireType.WIRETYPE_LENGTH_DELIMITED: {
                  keys.add(read(in));
                  break;
               }
               case 2 << 3 | WireType.WIRETYPE_LENGTH_DELIMITED: {
                  values.add(read(in));
                  break;
               }
               default: {
                  if (!in.skipField(tag)) done = true;
               }
            }
         }
         HashMap<Object, Object> map = new HashMap<>(keys.size());
         for (int i = 0; i < keys.size(); i++) {
            Object key = keys.get(i);
            Object value = values.get(i);
            map.put(key, value);
         }
         return new MarshallableMap<>(map);
      }

      @Override
      public void write(WriteContext ctx, MarshallableMap<?, ?> wrapper) throws IOException {
         Map<?, ?> map = wrapper.get();
         if (map == null) return;

         for (Map.Entry<?, ?> entry : map.entrySet()) {
            write(ctx, 1, entry.getKey());
            write(ctx, 2, entry.getValue());
         }
      }

      @Override
      public Class getJavaClass() {
         return MarshallableMap.class;
      }

      private Object read(TagReader in) throws IOException {
         try {
            byte[] bytes = in.readByteArray();
            return bytes.length == 0 ? null : marshaller.objectFromByteBuffer(bytes);
         } catch (ClassNotFoundException e) {
            throw new MarshallingException(e);
         }
      }

      private void write(WriteContext ctx, int field, Object object) throws IOException {
         try {
            // If object is null, write an empty byte array so that the null value can be recreated on the receiver.
            byte[] bytes = object == null ? Util.EMPTY_BYTE_ARRAY : marshaller.objectToByteBuffer(object);
            ctx.getWriter().writeBytes(field, bytes);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MarshallingException(e);
         }
      }
   }
}
