package org.infinispan.commons.util;

/**
 *
 * @param <K> The key type for this entry size calculator
 * @param <V> The value type for this entry size calculator
 */
@FunctionalInterface
public interface EntrySizeCalculator<K, V> {
   /**
    * Method used to calculate how much memory in size the key and value use.
    * @param key The key for this entry to be used in size calculation
    * @param value The value for this entry to be used in size calculation
    * @return The size approximately in memory the key and value use
    */
   long calculateSize(K key, V value);
}
