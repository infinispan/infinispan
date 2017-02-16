package org.infinispan.marshall.core;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.util.HopscotchHashMap;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.entries.InternalCacheValue;

class ClassIdentifiers implements ClassIds {
   // the hashmap is not changed after static ctor, therefore concurrent access is safe
   private final Map<Class<?>, Integer> classToId = new HopscotchHashMap<>(MAX_ID);
   private final Class<?>[] internalIdToClass;
   // for external ids we'll probably use Map<Integer, Class<?>> instead of array

   public static ClassIdentifiers load(GlobalConfiguration globalConfiguration) {
      return new ClassIdentifiers();
   }

   private ClassIdentifiers() {
      add(Object.class, OBJECT);
      add(String.class, STRING);
      add(List.class, LIST);
      add(Map.Entry.class, MAP_ENTRY);

      add(InternalCacheValue.class, INTERNAL_CACHE_VALUE);

      internalIdToClass = new Class[MAX_ID];
      classToId.entrySet().stream().forEach(e -> internalIdToClass[e.getValue().intValue()] = e.getKey());
   }

   private void add(Class<?> clazz, int id) {
      Integer prev = classToId.put(clazz, id);
      assert prev == null;
   }

   /**
    * This method throws IOException because it is assumed that we got the id from network.
    * @param id
    * @return
    * @throws IOException
    */
   public Class<?> getClass(int id) throws IOException {
      if (id < 0 || id > internalIdToClass.length) {
         throw new IOException("Unknown class id " + id);
      }
      Class<?> clazz = internalIdToClass[id];
      if (clazz == null) {
         throw new IOException("Unknown class id " + id);
      }
      return clazz;
   }

   /**
    * @param clazz
    * @return -1 if the id for given class is not found
    */
   public int getId(Class<?> clazz) {
      Integer id = classToId.get(clazz);
      if (id == null) {
         assert ExternallyMarshallable.isAllowed(clazz) : "Check support for " + clazz;
         return -1;
      } else {
         return id.intValue();
      }
   }
}
