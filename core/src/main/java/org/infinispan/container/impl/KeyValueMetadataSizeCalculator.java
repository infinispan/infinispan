package org.infinispan.container.impl;

import org.infinispan.metadata.Metadata;

/**
 * Size calculator that takes into account not only key and value but also metadata.
 * @author wburns
 * @since 9.0
 */
@FunctionalInterface
public interface KeyValueMetadataSizeCalculator<K, V> {

   /**
    * Method used to calculate how much memory in size the key, value and metadata use.
    * @param key The key for this entry to be used in size calculation
    * @param value The value for this entry to be used in size calculation
    * @param metadata The metadata for this entry to be used in size calculation
    * @return The size approximately in memory the key, value and metadata use.
    */
   long calculateSize(K key, V value, Metadata metadata);
}
