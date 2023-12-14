package org.infinispan.server.resp.filter;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.marshall.MarshallUtil.CollectionBuilder;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.Util;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.server.resp.ExternalizerIds;

public class EventListenerKeysFilter implements CacheEventFilter<Object, Object> {
   public static AdvancedExternalizer<EventListenerKeysFilter> EXTERNALIZER = new EventListenerKeysFilter.Externalizer();

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

   EventListenerKeysFilter(List<byte[]> keys) {
      this(keys.toArray(Util.EMPTY_BYTE_ARRAY_ARRAY));
   }

   public EventListenerKeysFilter(byte[] key) {
      this.keys = Map.of(key.length, Collections.singletonList(key));
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

   private static class Externalizer extends AbstractExternalizer<EventListenerKeysFilter> {

      @Override
      public Set<Class<? extends EventListenerKeysFilter>> getTypeClasses() {
         return Collections.singleton(EventListenerKeysFilter.class);
      }

      @Override
      public void writeObject(ObjectOutput output, EventListenerKeysFilter object) throws IOException {
         List<byte[]> keys = object.keys.values().stream().flatMap(List::stream).collect(Collectors.toList());
         MarshallUtil.marshallCollection(keys, output);
      }

      @Override
      public EventListenerKeysFilter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         List<byte[]> keys = (List<byte[]>)MarshallUtil.unmarshallCollection(input, (CollectionBuilder<byte[], List<byte[]>>)ArrayList::new);
         return new EventListenerKeysFilter(keys);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.EVENT_LISTENER_FILTER;
      }
   }
}
