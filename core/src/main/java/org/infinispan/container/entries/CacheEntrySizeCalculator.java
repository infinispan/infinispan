package org.infinispan.container.entries;

import org.infinispan.commons.util.concurrent.jdk8backported.AbstractEntrySizeCalculatorHelper;
import org.infinispan.commons.util.concurrent.jdk8backported.EntrySizeCalculator;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataMortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataTransientCacheEntry;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheEntry;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;

/**
 * Implementation of a size calculator that calcultes only the size of the value assuming it is an InternalCacheEntry.
 * This delegates the calculation of the key and the value contained within the InternalCacheEntry to the provided
 * SizeCalculator.
 * @param <K> The type of the key
 * @param <V> The type of the value
 * @author William Burns
 * @since 8.0
 */
public class CacheEntrySizeCalculator<K, V> extends AbstractEntrySizeCalculatorHelper<K, InternalCacheEntry<K, V>> {
   public CacheEntrySizeCalculator(EntrySizeCalculator<? super K, ? super V> calculator) {
      this.calculator = calculator;
   }

   private final EntrySizeCalculator<? super K, ? super V> calculator;

   @Override
   public long calculateSize(K key, InternalCacheEntry<K, V> ice) {
      long objSize = calculator.calculateSize(key, ice.getValue());
      long iceSize = 0;
      long metadataSize = 0;
      // ICE itself is an object and has a reference to it's class
      iceSize += OBJECT_SIZE + POINTER_SIZE;
      // Each ICE references key and value
      iceSize += 2 * POINTER_SIZE;
      boolean mortalEntry;
      boolean transientEntry;
      boolean metadataAware;
      // We want to put immortal entries first as they are very common.  Also MetadataImmortalCacheEntry extends
      // ImmortalCacheEntry so it has to come before
      if (ice instanceof MetadataImmortalCacheEntry) {
         mortalEntry = false;
         transientEntry = false;
         metadataAware = true;
      } else if (ice instanceof ImmortalCacheEntry) {
         mortalEntry = false;
         transientEntry = false;
         metadataAware = false;
      } else if (ice instanceof MortalCacheEntry) {
         mortalEntry = true;
         transientEntry = false;
         metadataAware = false;
      } else if (ice instanceof TransientCacheEntry) {
         mortalEntry = false;
         transientEntry = true;
         metadataAware = false;
      } else if (ice instanceof TransientMortalCacheEntry) {
         mortalEntry = true;
         transientEntry = true;
         metadataAware = false;
      } else if (ice instanceof MetadataMortalCacheEntry) {
         mortalEntry = true;
         transientEntry = false;
         metadataAware = true;
      } else if (ice instanceof MetadataTransientCacheEntry) {
         mortalEntry = false;
         transientEntry = true;
         metadataAware = true;
      } else if (ice instanceof MetadataTransientMortalCacheEntry) {
         mortalEntry = true;
         transientEntry = true;
         metadataAware = true;
      } else {
         mortalEntry = false;
         transientEntry = false;
         metadataAware = false;
      }
      if (metadataAware) {
         // Assume it has a pointer for the metadata
         iceSize += POINTER_SIZE;
         // The metadata has itself and the class reference
         metadataSize += OBJECT_SIZE + POINTER_SIZE;
         Metadata metadata = ice.getMetadata();
         if (metadata instanceof EmbeddedMetadata) {
            // The embedded metadata has a reference and NumericVersion instance
            metadataSize += POINTER_SIZE;
            metadataSize = roundUpToNearest8(metadataSize);
            // This is for the NumericVersion and the long inside of it
            metadataSize += OBJECT_SIZE + POINTER_SIZE + 8;
            metadataSize = roundUpToNearest8(metadataSize);
         } else {
            metadataSize = roundUpToNearest8(metadataSize);
         }
      }
      // Mortal uses 2 longs to keep track of created and lifespan
      iceSize += mortalEntry ? 16 : 0;
      // Transient uses 2 longs to keep track of last access and max idle
      iceSize += transientEntry ? 16 : 0;
      return objSize + roundUpToNearest8(iceSize) + metadataSize;
   }
}
