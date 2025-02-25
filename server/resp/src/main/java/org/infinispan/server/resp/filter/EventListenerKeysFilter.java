package org.infinispan.server.resp.filter;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.RESP_EVENT_LISTENER_KEYS_FILTER)
public class EventListenerKeysFilter implements CacheEventFilter<Object, Object> {

   private final Map<Integer, List<byte[]>> keys;

   public EventListenerKeysFilter(byte[] key) {
      this.keys = Map.of(key.length, List.of(key));
   }

   @ProtoFactory
   public EventListenerKeysFilter(Stream<byte[]> keys) {
      this.keys = keys.collect(
            Collectors.groupingBy(
                  k -> k.length,
                  Collectors.mapping(Function.identity(), Collectors.toList())
            )
      );
   }

   @ProtoField(1)
   Stream<byte[]> getKeys() {
      return keys.values().stream().flatMap(List::stream);
   }

   @Override
   public boolean accept(Object key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
      byte[] converted = key instanceof WrappedByteArray
            ? ((WrappedByteArray) key).getBytes()
            : (byte[]) key;
      List<byte[]> candidates = keys.get(converted.length);

      if (candidates == null) return false;

      for (byte[] k : candidates) {
         if (Arrays.equals(k, converted)) {
            return true;
         }
      }
      return false;
   }

   @Override
   public MediaType format() {
      return null;
   }
}
