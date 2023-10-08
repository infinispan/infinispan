package org.infinispan.server.resp.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.Util;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.RESP_EVENT_LISTENER_KEYS_FILTER)
public class EventListenerKeysFilter implements CacheEventFilter<Object, Object> {

   private final Map<Integer, List<byte[]>> keys;

   public EventListenerKeysFilter(byte[][] keys) {
      this.keys = new HashMap<>();
      for (byte[] key : keys) {
         this.keys.compute(key.length, (ignore, arr) -> {
            if (arr == null) {
               arr = new ArrayList<>();
            }
            arr.add(key);
            return arr;
         });
      }
   }

   public EventListenerKeysFilter(byte[] key) {
      List<byte[]> list = new ArrayList<>(1);
      list.add(key);
      this.keys = Map.of(key.length, list);
   }

   @ProtoFactory
   EventListenerKeysFilter(List<byte[]> keys) {
      this(keys.toArray(Util.EMPTY_BYTE_ARRAY_ARRAY));
   }

   @ProtoField(1)
   List<byte[]> getKeys() {
      return keys.values().stream().flatMap(List::stream).collect(Collectors.toList());
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
