package org.infinispan.functional.impl;

import org.infinispan.functional.FunctionalMap;

/**
 * Helper Factory to create FunctionalMaps
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class FunctionalMapFactory {

   public static <K, V> FunctionalMap.ReadOnlyMap<K, V> readOnlyMap(FunctionalMapImpl<K, V> functionalMap) {
      return ReadOnlyMapImpl.create(functionalMap);
   }

   public static <K, V> FunctionalMap.WriteOnlyMap<K, V> writeOnlyMap(FunctionalMapImpl<K, V> functionalMap) {
      return WriteOnlyMapImpl.create(functionalMap);
   }

   public static <K, V> FunctionalMap.ReadWriteMap<K, V> readWriteMap(FunctionalMapImpl<K, V> functionalMap) {
      return ReadWriteMapImpl.create(functionalMap);
   }
}
