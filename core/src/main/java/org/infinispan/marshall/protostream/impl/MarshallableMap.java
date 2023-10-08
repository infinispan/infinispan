package org.infinispan.marshall.protostream.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.ProtobufTagMarshaller;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.protostream.annotations.impl.GeneratedMarshallerBase;
import org.infinispan.protostream.descriptors.WireType;
import org.infinispan.protostream.impl.BaseMarshallerDelegate;
import org.infinispan.protostream.impl.SerializationContextImpl;
import org.infinispan.util.KeyValuePair;

/**
 * A wrapper for Maps of user objects whose key/value type is unknown until runtime.
 *
 * @author Ryan Emerson
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MARSHALLABLE_MAP)
// TODO replace List<KeyValuePair> with List<MarshallableObject<?>> for both keys & values
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
   MarshallableMap(List<KeyValuePair<K, V>> entries) {
      // no-op never actually used, as we override the default marshaller
      throw illegalState();
   }

   private MarshallableMap(Map<K, V> map) {
      this.map = map;
   }

   @ProtoField(number = 1)
   List<KeyValuePair<K, V>> getEntries() {
      throw illegalState();
   }

   public Map<K, V> get() {
      return map;
   }

   private IllegalStateException illegalState() {
      return new IllegalStateException(this.getClass().getSimpleName() + " marshaller not overridden in SerializationContext");
   }

   public static class Marshaller extends GeneratedMarshallerBase implements ProtobufTagMarshaller<MarshallableMap<?, ?>> {
      private BaseMarshallerDelegate<KeyValuePair> kvpMarshaller;
      private final String typeName;

      public Marshaller(String typeName) {
         this.typeName = typeName;
      }

      @Override
      public String getTypeName() {
         return typeName;
      }

      @Override
      public MarshallableMap<?, ?> read(ReadContext ctx) throws IOException {
         final org.infinispan.protostream.TagReader in = ctx.getReader();
         if (kvpMarshaller == null)
            kvpMarshaller = ((SerializationContextImpl) ctx.getSerializationContext()).getMarshallerDelegate(org.infinispan.util.KeyValuePair.class);

         Map<Object, Object> map = new HashMap<>();
         boolean done = false;
         while (!done) {
            final int tag = in.readTag();
            switch (tag) {
               case 0:
                  done = true;
                  break;
               case 1 << 3 | WireType.WIRETYPE_LENGTH_DELIMITED: {
                  int length = in.readUInt32();
                  int oldLimit = in.pushLimit(length);
                  KeyValuePair<?,?> kvp = readMessage(kvpMarshaller, ctx);
                  in.checkLastTagWas(0);
                  in.popLimit(oldLimit);
                  map.put(kvp.getKey(), kvp.getValue());
                  break;
               }
               default: {
                  if (!in.skipField(tag)) done = true;
               }
            }
         }
         return new MarshallableMap<>(map);
      }

      @Override
      public void write(WriteContext ctx, MarshallableMap<?, ?> wrapper) throws IOException {
         if (kvpMarshaller == null)
            kvpMarshaller = ((SerializationContextImpl) ctx.getSerializationContext()).getMarshallerDelegate(org.infinispan.util.KeyValuePair.class);

         Map<?, ?> map = wrapper.get();
         if (map != null) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
               // TODO is this the best way to represent entries?
               // It means that key and value are always represented as MarshallableObject
               // Just use custom Entry class that stores bytes for both?
               // Saving of 2 bytes per entry
               // MarshallableMap passes GlobalMarshaller
               // MarshallableUserMap then passes just the user marshaller
               KeyValuePair<?, ?> kvp = new KeyValuePair<>(entry.getKey(), entry.getValue());
               writeNestedMessage(kvpMarshaller, ctx, 1, kvp);
            }
         }
      }

      @Override
      public Class getJavaClass() {
         return MarshallableMap.class;
      }
   }
}
