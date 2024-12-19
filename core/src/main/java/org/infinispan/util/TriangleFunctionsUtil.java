package org.infinispan.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.remoting.responses.ValidResponse;

/**
 * Some utility functions for {@link org.infinispan.interceptors.distribution.TriangleDistributionInterceptor}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public final class TriangleFunctionsUtil {

   private TriangleFunctionsUtil() {
   }

   public static PutMapCommand copy(PutMapCommand command, Collection<Object> keys) {
      PutMapCommand copy = new PutMapCommand(command);
      copy.setMap(filterEntries(command.getMap(), keys));
      return copy;
   }

   public static <K, V, T> WriteOnlyManyEntriesCommand<K, V, T> copy(WriteOnlyManyEntriesCommand<K, V, T> command,
         Collection<Object> keys) {
      return new WriteOnlyManyEntriesCommand<>(command).withArguments(filterEntries(command.getArguments(), keys));
   }

   public static <K, V> WriteOnlyManyCommand<K, V> copy(WriteOnlyManyCommand<K, V> command, Collection<Object> keys) {
      WriteOnlyManyCommand<K, V> copy = new WriteOnlyManyCommand<>(command);
      copy.setKeys(keys);
      return copy;
   }

   public static <K, V, R> ReadWriteManyCommand<K, V, R> copy(ReadWriteManyCommand<K, V, R> command,
         Collection<Object> keys) {
      ReadWriteManyCommand<K, V, R> copy = new ReadWriteManyCommand<>(command);
      copy.setKeys(keys);
      return copy;
   }

   public static <K, V, T, R> ReadWriteManyEntriesCommand<K, V, T, R> copy(ReadWriteManyEntriesCommand<K, V, T, R> command,
         Collection<Object> keys) {
      return new ReadWriteManyEntriesCommand<K, V, T, R>(command).withArguments(filterEntries(command.getArguments(), keys));
   }

   public static Map<Object, Object> mergeHashMap(ValidResponse response, Map<Object, Object> resultMap) {
      return InfinispanCollections.mergeMaps(resultMap, response.getResponseMap());
   }

   @SuppressWarnings("unused")
   public static Void voidMerge(ValidResponse ignored1, Void ignored2) {
      return null;
   }

   public static List<Object> mergeList(ValidResponse response, List<Object> resultList) {
      List<Object> list = (List<Object>) response.getResponseCollection();
      return InfinispanCollections.mergeLists(list, resultList);
   }

   public static Map<Integer, Collection<Object>> filterBySegment(LocalizedCacheTopology cacheTopology,
         Collection<Object> keys) {
      Map<Integer, Collection<Object>> filteredKeys = new HashMap<>(
            cacheTopology.getReadConsistentHash().getNumSegments());
      for (Object key : keys) {
         filteredKeys.computeIfAbsent(cacheTopology.getSegment(key), integer -> new ArrayList<>()).add(key);
      }
      return filteredKeys;
   }


   public static <K, V> Map<K, V> filterEntries(Map<K, V> map, Collection<Object> keys) {
      //note: can't use Collector.toMap() since the implementation doesn't support null values.
      return map.entrySet().stream()
            .filter(entry -> keys.contains(entry.getKey()))
            .collect(HashMap::new, (rMap, entry) -> rMap.put(entry.getKey(), entry.getValue()), HashMap::putAll);
   }
}
