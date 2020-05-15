package org.infinispan.container.entries;

import org.infinispan.commons.util.AbstractEntrySizeCalculatorHelper;
import org.infinispan.commons.util.EntrySizeCalculator;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataMortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataTransientCacheEntry;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheEntry;
import org.infinispan.container.impl.InternalEntryFactoryImpl;
import org.infinispan.container.impl.KeyValueMetadataSizeCalculator;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;

/**
 * Implementation of a size calculator that calculates only the size of the value assuming it is an InternalCacheEntry.
 * This delegates the calculation of the key and the value contained within the InternalCacheEntry to the provided
 * SizeCalculator.
 * @param <K> The type of the key
 * @param <V> The type of the value
 * @author William Burns
 * @since 8.0
 */
public class CacheEntrySizeCalculator<K, V> extends AbstractEntrySizeCalculatorHelper<K, InternalCacheEntry<K, V>>
      implements KeyValueMetadataSizeCalculator<K, V> {
   public CacheEntrySizeCalculator(EntrySizeCalculator<? super K, ? super V> calculator) {
      this.calculator = calculator;
   }

   private final EntrySizeCalculator<? super K, ? super V> calculator;

   @Override
   public long calculateSize(K key, InternalCacheEntry<K, V> ice) {
      // This will be non zero when use expiration, but don't want to store the metadata
      long noMetadataSize = 0;
      boolean metadataAware;
      // We want to put immortal entries first as they are very common.  Also MetadataImmortalCacheEntry extends
      // ImmortalCacheEntry so it has to come before
      if (ice instanceof MetadataImmortalCacheEntry) {
         metadataAware = true;
      } else if (ice instanceof ImmortalCacheEntry) {
         metadataAware = false;
      } else if (ice instanceof MortalCacheEntry) {
         noMetadataSize += 16;
         metadataAware = false;
      } else if (ice instanceof TransientCacheEntry) {
         noMetadataSize += 16;
         metadataAware = false;
      } else if (ice instanceof TransientMortalCacheEntry) {
         noMetadataSize += 32;
         metadataAware = false;
      } else if (ice instanceof MetadataMortalCacheEntry) {
         metadataAware = true;
      } else if (ice instanceof MetadataTransientCacheEntry) {
         metadataAware = true;
      } else {
         metadataAware = ice instanceof MetadataTransientMortalCacheEntry;
      }
      Metadata metadata;
      if (metadataAware) {
         metadata = ice.getMetadata();
         // We don't support other metadata types currently
         if (!(metadata instanceof EmbeddedMetadata)) {
            metadata = null;
         }
      } else {
         metadata = null;
      }
      long keyValueMetadataSize = calculateSize(key, ice.getValue(), metadata, ice.getInternalMetadata());
      return keyValueMetadataSize + noMetadataSize;
   }

   @Override
   public long calculateSize(K key, V value, Metadata metadata, PrivateMetadata pvtMetadata) {
      long objSize = calculator.calculateSize(key, value);

      // This is for the surrounding ICE
      long iceSize = 0;
      // ICE itself is an object and has a reference to it's class
      iceSize += OBJECT_SIZE + POINTER_SIZE;
      // Each ICE references key and value and private metadata
      iceSize += 3 * POINTER_SIZE;

      long metadataSize = 0;
      if (metadata != null) {
         // Mortal uses 2 longs to keep track of created and lifespan
         if (metadata.lifespan() != -1) {
            iceSize += 16;
         }
         // Transient uses 2 longs to keep track of last access and max idle
         if (metadata.maxIdle() != -1) {
            iceSize += 16;
         }
         if (InternalEntryFactoryImpl.isStoreMetadata(metadata, null)) {
            // Assume it has a pointer for the metadata
            iceSize += POINTER_SIZE;
            // The metadata has itself and the class reference
            metadataSize += OBJECT_SIZE + POINTER_SIZE;
            // We only support embedded metadata that has a reference and NumericVersion instance
            metadataSize += POINTER_SIZE;
            metadataSize = roundUpToNearest8(metadataSize);
            // This is for the NumericVersion and the long inside of it
            metadataSize += numericVersionSize();
            metadataSize = roundUpToNearest8(metadataSize);
         }
      }
      long pvtMetadataSize = pvtMetadata == null || pvtMetadata.isEmpty() ? 0 : privateMetadataSize(pvtMetadata);

      return objSize + roundUpToNearest8(iceSize) + metadataSize + pvtMetadataSize;
   }

   private static long privateMetadataSize(PrivateMetadata metadata) {
      long size = HEADER_AND_CLASS_REFERENCE;
      size += 2 * POINTER_SIZE; //two fields, IracMetadata & EntryVersion
      size = roundUpToNearest8(size);
      if (metadata.iracMetadata() != null) {
         size += iracMetadataSize();
      }
      if (metadata.getNumericVersion() != null) {
         size += numericVersionSize();
      } else if (metadata.getClusteredVersion() != null) {
         size += simpleClusteredVersionSize();
      }
      return size;
   }

   private static long iracMetadataSize() {
      //estimated
      long size = HEADER_AND_CLASS_REFERENCE;
      size += 2 * POINTER_SIZE; //site: String, version: IracEntryVersion
      //go recursive?
      return roundUpToNearest8(size);
   }

   private static long numericVersionSize() {
      //only a long stored
      return roundUpToNearest8(HEADER_AND_CLASS_REFERENCE + 8);
   }

   private static long simpleClusteredVersionSize() {
      //only a int and long
      return roundUpToNearest8(HEADER_AND_CLASS_REFERENCE + 4 + 8);
   }
}
