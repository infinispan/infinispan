package org.infinispan.commons.util.concurrent.jdk8backported;

import java.util.Map;

import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.concurrent.ParallelIterableMap;
import org.infinispan.commons.util.concurrent.ParallelIterableMap.KeyValueAction;
import org.infinispan.commons.util.concurrent.jdk8backported.EquivalentConcurrentHashMapV8.BiAction;

public class ConcurrentParallelHashMapV8<K, V> extends EquivalentConcurrentHashMapV8<K, V> implements
      ParallelIterableMap<K, V> {

   /**
    * 
    */
   private static final long serialVersionUID = 7306507951179792494L;

   public ConcurrentParallelHashMapV8(Equivalence<? super K> keyEquivalence, Equivalence<? super V> valueEquivalence) {
      super(keyEquivalence, valueEquivalence);
   }

   public ConcurrentParallelHashMapV8(int initialCapacity, Equivalence<? super K> keyEquivalence,
         Equivalence<? super V> valueEquivalence) {
      super(initialCapacity, keyEquivalence, valueEquivalence);
   }

   public ConcurrentParallelHashMapV8(int initialCapacity, float loadFactor, Equivalence<? super K> keyEquivalence,
         Equivalence<? super V> valueEquivalence) {
      super(initialCapacity, loadFactor, keyEquivalence, valueEquivalence);
   }

   public ConcurrentParallelHashMapV8(int initialCapacity, float loadFactor, int concurrencyLevel,
         Equivalence<? super K> keyEquivalence, Equivalence<? super V> valueEquivalence) {
      super(initialCapacity, loadFactor, concurrencyLevel, keyEquivalence, valueEquivalence);
   }

   public ConcurrentParallelHashMapV8(Map<? extends K, ? extends V> m, Equivalence<? super K> keyEquivalence,
         Equivalence<? super V> valueEquivalence) {
      super(m, keyEquivalence, valueEquivalence);
   }

   public void forEach(long parallelismThreshold, final KeyValueAction<? super K, ? super V> action)
         throws InterruptedException {
      forEach(parallelismThreshold, new BiAction<K, V>() {

         @Override
         public void apply(K a, V b) {
            action.apply(a, b);
         }
      });
   }
}
