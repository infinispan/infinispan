package org.infinispan.marshall.protostream.impl;

import static org.infinispan.marshall.protostream.impl.GlobalContextInitializer.getFqTypeName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.ProtobufTagMarshaller;
import org.infinispan.protostream.TagReader;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.protostream.annotations.impl.GeneratedMarshallerBase;
import org.infinispan.protostream.descriptors.WireType;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A wrapper for Maps of user objects whose key/value type is unknown until runtime.
 *
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MARSHALLABLE_MAP)
public class MarshallableMap<K, V> {

   static final Log log = LogFactory.getLog(MarshallableMap.class);

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
      throw log.marshallerNotOverridden(getClass().getName());
   }

   private MarshallableMap(Map<K, V> map) {
      this.map = map;
   }

   @ProtoField(1)
   List<byte[]> getKeys() {
      throw log.marshallerNotOverridden(getClass().getName());
   }

   @ProtoField(2)
   List<byte[]> getValues() {
      throw log.marshallerNotOverridden(getClass().getName());
   }

   public Map<K, V> get() {
      return map;
   }


   public static class Marshaller extends GeneratedMarshallerBase implements ProtobufTagMarshaller<MarshallableMap<?, ?>> {
      private final String typeName;
      private final AbstractInternalProtoStreamMarshaller marshaller;

      public Marshaller(AbstractInternalProtoStreamMarshaller marshaller) {
         this.typeName = getFqTypeName(MarshallableMap.class);
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
               case 1 << WireType.TAG_TYPE_NUM_BITS | WireType.WIRETYPE_LENGTH_DELIMITED: {
                  keys.add(read(in));
                  break;
               }
               case 2 << WireType.TAG_TYPE_NUM_BITS | WireType.WIRETYPE_LENGTH_DELIMITED: {
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
         byte[] bytes = in.readByteArray();
         return bytes.length == 0 ? null : marshaller.objectFromByteBuffer(bytes);
      }

      private void write(WriteContext ctx, int field, Object object) throws IOException {
         // If object is null, write an empty byte array so that the null value can be recreated on the receiver.
         if (object == null) {
            ctx.getWriter().writeBytes(field, Util.EMPTY_BYTE_ARRAY);
         } else {
            ByteBuffer buf = marshaller.objectToBuffer(object);
            ctx.getWriter().writeBytes(field, buf.getBuf(), buf.getOffset(), buf.getLength());
         }
      }
   }
}
