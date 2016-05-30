package org.infinispan.query.continuous;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.query.api.continuous.ContinuousQueryListener;

/**
 * {@link ContinuousQueryListener} which counts number of calls for each key.
 *
 * @author vjuranek
 * @since 8.0
 */
public class CallCountingCQResultListener<K, V> implements ContinuousQueryListener<K, V> {

   private final Map<K, Integer> joined = new HashMap<>();
   private final Map<K, Integer> updated = new HashMap<>();
   private final Map<K, Integer> left = new HashMap<>();

   @Override
   public void resultJoining(K key, V value) {
      incrementNumberOfCalls(key, joined);
   }

   @Override
   public void resultUpdated(K key, V value) {
      incrementNumberOfCalls(key, updated);
   }

   @Override
   public void resultLeaving(K key) {
      incrementNumberOfCalls(key, left);
   }

   public Map<K, Integer> getJoined() {
      return joined;
   }

   public Map<K, Integer> getUpdated() {
      return updated;
   }

   public Map<K, Integer> getLeft() {
      return left;
   }

   private void incrementNumberOfCalls(K key, Map<K, Integer> callMap) {
      synchronized (callMap) {
         Integer calls = callMap.get(key);
         callMap.put(key, calls == null ? 1 : calls + 1);
      }
   }
}
