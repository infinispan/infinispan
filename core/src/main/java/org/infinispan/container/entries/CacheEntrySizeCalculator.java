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
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;

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

   private static final long IMMORTAL_ENTRY_SIZE = ClassLayout.parseInstance(new ImmortalCacheEntry(null, null)).instanceSize();
   private static final long MORTAL_ENTRY_SIZE = ClassLayout.parseInstance(new MortalCacheEntry(null, null, 0, 0)).instanceSize();
   private static final long TRANSIENT_ENTRY_SIZE = ClassLayout.parseInstance(new TransientCacheEntry(null, null, 0, 0)).instanceSize();
   private static final long TRANSIENT_MORTAL_ENTRY_SIZE = ClassLayout.parseInstance(new TransientMortalCacheEntry(null, null, 0, 0, 0, 0)).instanceSize();
   private static final long METADATA_IMMORTAL_ENTRY_SIZE = ClassLayout.parseInstance(new MetadataImmortalCacheEntry(null, null, null)).instanceSize();
   private static final long METADATA_MORTAL_ENTRY_SIZE = ClassLayout.parseInstance(new MetadataMortalCacheEntry(null, null, null, 0)).instanceSize();
   private static final long METADATA_TRANSIENT_ENTRY_SIZE = ClassLayout.parseInstance(new MetadataTransientCacheEntry(null, null, null, 0)).instanceSize();
   private static final long METADATA_TRANSIENT_MORTAL_ENTRY_SIZE = ClassLayout.parseInstance(new MetadataTransientMortalCacheEntry(null, null, null, 0, 0)).instanceSize();

   @Override
   public long calculateSize(K key, InternalCacheEntry<K, V> ice) {
      boolean metadataAware;
      // We want to put immortal entries first as they are very common.  Also MetadataImmortalCacheEntry extends
      // ImmortalCacheEntry so it has to come before
      if (ice instanceof MetadataImmortalCacheEntry) {
         metadataAware = true;
      } else if (ice instanceof ImmortalCacheEntry) {
         metadataAware = false;
      } else if (ice instanceof MortalCacheEntry) {
         metadataAware = false;
      } else if (ice instanceof TransientCacheEntry) {
         metadataAware = false;
      } else if (ice instanceof TransientMortalCacheEntry) {
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

      long keyValueSize = calculator.calculateSize(key, ice.getValue());

      long metadataSize = 0;
      if (metadata != null) {
         if (InternalEntryFactoryImpl.isStoreMetadata(metadata, null)) {
            metadataSize = GraphLayout.parseInstance(metadata).totalSize();
         }
      }

      long internalMetadataSize = ice.getInternalMetadata() == null || ice.getInternalMetadata().isEmpty() ? 0 : GraphLayout.parseInstance(ice.getInternalMetadata()).totalSize();

      return keyValueSize + ClassLayout.parseInstance(ice).instanceSize() + metadataSize + internalMetadataSize;
   }
   @Override
   public long calculateSize(K key, V value, Metadata metadata, PrivateMetadata pvtMetadata) {
      long objSize = calculator.calculateSize(key, value);

      long iceOverhead = estimateIceOverhead(metadata);

      long metadataSize = 0;
      if (metadata != null) {
         if (InternalEntryFactoryImpl.isStoreMetadata(metadata, null)) {
            metadataSize = GraphLayout.parseInstance(metadata).totalSize();
         }
      }
      long pvtMetadataSize = pvtMetadata == null || pvtMetadata.isEmpty() ? 0 : GraphLayout.parseInstance(pvtMetadata).totalSize();

      return objSize + iceOverhead + metadataSize + pvtMetadataSize;
   }

   private long estimateIceOverhead(Metadata metadata) {
      long size = IMMORTAL_ENTRY_SIZE;
      boolean lifespan = metadata != null && metadata.lifespan() != -1;
      boolean maxIdle = metadata != null && metadata.maxIdle() != -1;

      if (lifespan && maxIdle) {
         size = TRANSIENT_MORTAL_ENTRY_SIZE;
      } else if (lifespan) {
         size = MORTAL_ENTRY_SIZE;
      } else if (maxIdle) {
         size = TRANSIENT_ENTRY_SIZE;
      }

      if (InternalEntryFactoryImpl.isStoreMetadata(metadata, null)) {
          if (lifespan && maxIdle) {
             size = METADATA_TRANSIENT_MORTAL_ENTRY_SIZE;
         } else if (lifespan) {
             size = METADATA_MORTAL_ENTRY_SIZE;
         } else if (maxIdle) {
             size = METADATA_TRANSIENT_ENTRY_SIZE;
         } else {
             size = METADATA_IMMORTAL_ENTRY_SIZE;
         }
      }
      return size;
   }
}
