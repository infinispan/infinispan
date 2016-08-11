package org.infinispan.commons.util.concurrent.jdk8backported;

public enum Eviction {
    NONE {
        @Override
        public <K, V> EvictionPolicy<K, V> make(
              BoundedEquivalentConcurrentHashMapV8<K, V> map,
              EntrySizeCalculator<? super K, ? super V> sizeCalculator, long capacity) {
            return new NullEvictionPolicy<K, V>(map.nodeEq);
        }
    },
    LRU {
        @Override
        public <K, V> EvictionPolicy<K, V> make(
              BoundedEquivalentConcurrentHashMapV8<K, V> map,
              EntrySizeCalculator<? super K, ? super V> sizeCalculator, long capacity) {
            if (sizeCalculator == null) {
                return new LRUEvictionPolicy<K, V>(map, capacity,
                      SingleEntrySizeCalculator.SINGLETON, false);
            } else {
                return new LRUEvictionPolicy<K, V>(map, capacity,
                      new NodeSizeCalculatorWrapper<K, V>(sizeCalculator), true);
            }

        }
    },
    LIRS {
        @Override
        public <K, V> EvictionPolicy<K, V> make(BoundedEquivalentConcurrentHashMapV8<K, V> map,
                                                EntrySizeCalculator<? super K, ? super V> sizeCalculator, long capacity) {
            if (sizeCalculator != null) {
                throw new IllegalArgumentException("LIRS does not support a size calculator!");
            }
            return new LIRSEvictionPolicy<K, V>(map, capacity);
        }
    };

    abstract <K, V> EvictionPolicy<K, V> make(
          BoundedEquivalentConcurrentHashMapV8<K, V> map, EntrySizeCalculator<? super K, ? super V> sizeCalculator, long capacity);
}
