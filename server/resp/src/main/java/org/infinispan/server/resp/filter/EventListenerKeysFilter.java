package org.infinispan.server.resp.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.encoding.DataConversion;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.EventType;

public class EventListenerKeysFilter implements CacheEventFilter<Object, Object> {
   private final Map<Integer, List<byte[]>> keys;
   private final DataConversion keyConversion;

   public EventListenerKeysFilter(byte[][] keys, DataConversion conversion) {
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
      this.keyConversion = conversion;
   }

   public EventListenerKeysFilter(byte[] key, DataConversion conversion) {
      this.keys = Map.of(key.length, Collections.singletonList(key));
      this.keyConversion = conversion;
   }

   @Override
   public boolean accept(Object key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
      byte[] converted = (byte[]) keyConversion.fromStorage(key);
      List<byte[]> candidates = keys.get(converted.length);

      if (candidates == null) return false;

      for (byte[] k : candidates) {
         if (Arrays.equals(k, converted)) {
            return true;
         }
      }
      return false;
   }
}
