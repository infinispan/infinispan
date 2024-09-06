package org.infinispan.query.impl;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.encoding.DataConversion;

public class QueryKeyConverter {

   private final AdvancedCache<?, ?> cache;

   public QueryKeyConverter(AdvancedCache<?, ?> cache) {
      this.cache = cache;
   }

   public Set<Object> convertKeys(Collection<?> identifiers) {
      DataConversion keyDataConversion = cache.getKeyDataConversion();
      LinkedHashSet<Object> keys = new LinkedHashSet<>(identifiers.size());
      for (Object identifier : identifiers) {
         Object key = (useStorageEncoding()) ?
               keyDataConversion.toStorage(identifier) :
               keyDataConversion.fromStorage(identifier);
         keys.add(key);
      }
      return keys;
   }

   public <V> Map<?, V> convertEntries(Map<?, V> entries) {
      if (!useStorageEncoding()) {
         return entries;
      }

      DataConversion keyDataConversion = cache.getKeyDataConversion();
      LinkedHashMap<Object, V> converted = new LinkedHashMap<>();
      for (Map.Entry<?, V> entry : entries.entrySet()) {
         converted.put(keyDataConversion.toStorage(entry.getKey()), entry.getValue());
      }
      return converted;
   }

   public Object convert(Object key) {
      if (!useStorageEncoding()) {
         return key;
      }
      return cache.getKeyDataConversion().toStorage(key);
   }

   private boolean useStorageEncoding() {
      return MediaType.APPLICATION_PROTOSTREAM.equals(cache.getKeyDataConversion().getRequestMediaType());
   }
}
