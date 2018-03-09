package org.infinispan.container;

import java.util.function.LongSupplier;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.L1InternalCacheEntry;
import org.infinispan.container.entries.MortalCacheEntry;
import org.infinispan.container.entries.MortalCacheValue;
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.container.entries.TransientCacheValue;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheValue;
import org.infinispan.container.entries.metadata.L1MetadataInternalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataImmortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataMortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataMortalCacheValue;
import org.infinispan.container.entries.metadata.MetadataTransientCacheEntry;
import org.infinispan.container.entries.metadata.MetadataTransientCacheValue;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheEntry;
import org.infinispan.container.entries.metadata.MetadataTransientMortalCacheValue;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.TimeService;

/**
 * An implementation that generates non-versioned entries
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class InternalEntryFactoryImpl implements InternalEntryFactory {
   private static final int L = 1; // Lifespan
   private static final int I = 2; // max Idle
   private static final int M = 4; // Metadata

   @Inject private TimeService timeService;

   @Override
   public InternalCacheEntry create(Object key, Object value, Metadata metadata) {
      long lifespan = metadata != null ? metadata.lifespan() : -1;
      long maxIdle = metadata != null ? metadata.maxIdle() : -1;
      if (!isStoreMetadata(metadata, false)) {
         if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheEntry(key, value);
         if (lifespan > -1 && maxIdle < 0) return new MortalCacheEntry(key, value, lifespan, timeService.wallClockTime());
         if (lifespan < 0 && maxIdle > -1) return new TransientCacheEntry(key, value, maxIdle, timeService.wallClockTime());
         return new TransientMortalCacheEntry(key, value, maxIdle, lifespan, timeService.wallClockTime());
      } else {
         if (lifespan < 0 && maxIdle < 0) return new MetadataImmortalCacheEntry(key, value, metadata);
         if (lifespan > -1 && maxIdle < 0) return new MetadataMortalCacheEntry(key, value, metadata, timeService.wallClockTime());
         if (lifespan < 0 && maxIdle > -1) return new MetadataTransientCacheEntry(key, value, metadata, timeService.wallClockTime());
         return new MetadataTransientMortalCacheEntry(key, value, metadata, timeService.wallClockTime());
      }
   }

   @Override
   public InternalCacheEntry create(CacheEntry cacheEntry) {
      return create(cacheEntry.getKey(), cacheEntry.getValue(),
            cacheEntry.getMetadata(), cacheEntry.getLifespan(), cacheEntry.getMaxIdle());
   }

   @Override
   public InternalCacheEntry create(Object key, Object value, InternalCacheEntry cacheEntry) {
      return create(key, value, cacheEntry.getMetadata(), cacheEntry.getCreated(),
            cacheEntry.getLifespan(), cacheEntry.getLastUsed(), cacheEntry.getMaxIdle());
   }

   @Override
   public InternalCacheEntry create(Object key, Object value, EntryVersion version, long created, long lifespan, long lastUsed, long maxIdle) {
      if (version == null) {
         if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheEntry(key, value);
         if (lifespan > -1 && maxIdle < 0) return new MortalCacheEntry(key, value, lifespan, created);
         if (lifespan < 0 && maxIdle > -1) return new TransientCacheEntry(key, value, maxIdle, lastUsed);
         return new TransientMortalCacheEntry(key, value, maxIdle, lifespan, lastUsed, created);
      } else {
         // If no metadata passed, assumed embedded metadata
         Metadata metadata = new EmbeddedMetadata.Builder()
               .lifespan(lifespan).maxIdle(maxIdle).version(version).build();
         if (lifespan < 0 && maxIdle < 0) return new MetadataImmortalCacheEntry(key, value, metadata);
         if (lifespan > -1 && maxIdle < 0) return new MetadataMortalCacheEntry(key, value, metadata, created);
         if (lifespan < 0 && maxIdle > -1) return new MetadataTransientCacheEntry(key, value, metadata, lastUsed);
         return new MetadataTransientMortalCacheEntry(key, value, metadata, lastUsed, created);
      }
   }

   @Override
   public InternalCacheEntry create(Object key, Object value, Metadata metadata, long created, long lifespan, long lastUsed, long maxIdle) {
      if (!isStoreMetadata(metadata, false)) {
         if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheEntry(key, value);
         if (lifespan > -1 && maxIdle < 0) return new MortalCacheEntry(key, value, lifespan, created);
         if (lifespan < 0 && maxIdle > -1) return new TransientCacheEntry(key, value, maxIdle, lastUsed);
         return new TransientMortalCacheEntry(key, value, maxIdle, lifespan, lastUsed, created);
      } else {
         // Metadata to store, take lifespan and maxIdle settings from it
         long metaLifespan = metadata.lifespan();
         long metaMaxIdle = metadata.maxIdle();
         if (metaLifespan < 0 && metaMaxIdle < 0) return new MetadataImmortalCacheEntry(key, value, metadata);
         if (metaLifespan > -1 && metaMaxIdle < 0) return new MetadataMortalCacheEntry(key, value, metadata, created);
         if (metaLifespan < 0 && metaMaxIdle > -1) return new MetadataTransientCacheEntry(key, value, metadata, lastUsed);
         return new MetadataTransientMortalCacheEntry(key, value, metadata, lastUsed, created);
      }
   }

   @Override
   public InternalCacheValue createValue(CacheEntry cacheEntry, boolean includeInvocationRecords) {
      Metadata metadata = cacheEntry.getMetadata();
      long lifespan = cacheEntry.getLifespan();
      long maxIdle = cacheEntry.getMaxIdle();
      if (!isStoreMetadata(metadata, !includeInvocationRecords)) {
         if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheValue(cacheEntry.getValue());
         if (lifespan > -1 && maxIdle < 0) return new MortalCacheValue(cacheEntry.getValue(), -1, lifespan);
         if (lifespan < 0 && maxIdle > -1) return new TransientCacheValue(cacheEntry.getValue(), maxIdle, -1);
         return new TransientMortalCacheValue(cacheEntry.getValue(), -1, lifespan, maxIdle, -1);
      } else {
         metadata = metadata.builder().noInvocations().build();
         if (lifespan < 0 && maxIdle < 0) return new MetadataImmortalCacheValue(cacheEntry.getValue(), metadata);
         if (lifespan > -1 && maxIdle < 0) return new MetadataMortalCacheValue(cacheEntry.getValue(), metadata, -1);
         if (lifespan < 0 && maxIdle > -1) return new MetadataTransientCacheValue(cacheEntry.getValue(), metadata, -1);
         return new MetadataTransientMortalCacheValue(cacheEntry.getValue(), metadata, -1, -1);
      }
   }

   @Override
   // TODO: Do we need this???
   public InternalCacheEntry create(Object key, Object value, Metadata metadata, long lifespan, long maxIdle) {
      if (!isStoreMetadata(metadata, false)) {
         if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheEntry(key, value);
         if (lifespan > -1 && maxIdle < 0) return new MortalCacheEntry(key, value, lifespan, timeService.wallClockTime());
         if (lifespan < 0 && maxIdle > -1) return new TransientCacheEntry(key, value, maxIdle, timeService.wallClockTime());
         return new TransientMortalCacheEntry(key, value, maxIdle, lifespan, timeService.wallClockTime());
      } else {
         // Metadata to store, take lifespan and maxIdle settings from it
         long metaLifespan = metadata.lifespan();
         long metaMaxIdle = metadata.maxIdle();
         if (metaLifespan < 0 && metaMaxIdle < 0) return new MetadataImmortalCacheEntry(key, value, metadata);
         if (metaLifespan > -1 && metaMaxIdle < 0) return new MetadataMortalCacheEntry(key, value, metadata, timeService.wallClockTime());
         if (metaLifespan < 0 && metaMaxIdle > -1) return new MetadataTransientCacheEntry(key, value, metadata, timeService.wallClockTime());
         return new MetadataTransientMortalCacheEntry(key, value, metadata, timeService.wallClockTime());
      }
   }

   @Override
   public InternalCacheEntry update(InternalCacheEntry ice, Metadata metadata, boolean preserveTimestamps) {
      long lifespan = metadata.lifespan();
      long maxIdle = metadata.maxIdle();
      int mask = (lifespan < 0 ? 0 : L) | (maxIdle < 0 ? 0 : I) | (isStoreMetadata(metadata, false) ? M : 0);

      Object key = ice.getKey();
      Object value = ice.getValue();
      Class<? extends InternalCacheEntry> clazz = ice.getClass();
      switch (mask) {
         case 0:
            if (clazz == ImmortalCacheEntry.class) {
               return ice;
            }
            return new ImmortalCacheEntry(key, value);
         case L:
            if (clazz == MortalCacheEntry.class) {
               ((MortalCacheEntry) ice).setLifespan(lifespan);
               return ice;
            }
            return new MortalCacheEntry(key, value, lifespan, getCreated(ice, preserveTimestamps));
         case I:
            if (clazz == TransientCacheEntry.class) {
               ((TransientCacheEntry) ice).setMaxIdle(maxIdle);
               return ice;
            }
            return new TransientCacheEntry(key, value, maxIdle, getLastUsed(ice, preserveTimestamps));
         case L | I:
            if (clazz == TransientMortalCacheEntry.class) {
               TransientMortalCacheEntry transientMortalEntry = (TransientMortalCacheEntry) ice;
               transientMortalEntry.setLifespan(lifespan);
               transientMortalEntry.setMaxIdle(maxIdle);
               return ice;
            }
            long ctm1 = timeService.wallClockTime();
            return new TransientMortalCacheEntry(key, value, maxIdle, lifespan,
                  getCreated(ice, preserveTimestamps, () -> ctm1), getCreated(ice, preserveTimestamps, () -> ctm1));
         case M:
            if (clazz == MetadataImmortalCacheEntry.class) {
               ice.setMetadata(metadata);
               return ice;
            }
            return new MetadataImmortalCacheEntry(key, value, metadata);
         case L | M:
            if (clazz == MetadataMortalCacheEntry.class) {
               ice.setMetadata(metadata);
               return ice;
            }
            return new MetadataMortalCacheEntry(key, value, metadata, getCreated(ice, preserveTimestamps));
         case I | M:
            if (clazz == MetadataTransientCacheEntry.class) {
               ice.setMetadata(metadata);
               return ice;
            }
            return new MetadataTransientCacheEntry(key, value, metadata, getLastUsed(ice, preserveTimestamps));
         case L | I | M:
            if (clazz == MetadataTransientMortalCacheEntry.class) {
               ice.setMetadata(metadata);
               return ice;
            }
            long ctm2 = timeService.wallClockTime();
            return new MetadataTransientMortalCacheEntry(key, value, metadata, getCreated(ice, preserveTimestamps,
                  () -> ctm2), getLastUsed(ice, preserveTimestamps, () -> ctm2));
         default:
            throw new IllegalStateException();
      }
   }

   private long getCreated(InternalCacheEntry ice, boolean preserveTimestamps) {
      return getCreated(ice, preserveTimestamps, timeService::wallClockTime);
   }

   private long getCreated(InternalCacheEntry ice, boolean preserveTimestamps, LongSupplier timestamp) {
      if (preserveTimestamps && ice.getCreated() >= 0) {
         return ice.getCreated();
      } else {
         return timestamp.getAsLong();
      }
   }

   private long getLastUsed(InternalCacheEntry ice, boolean preserveTimestamps) {
      return getLastUsed(ice, preserveTimestamps, timeService::wallClockTime);
   }

   private long getLastUsed(InternalCacheEntry ice, boolean preserveTimestamps, LongSupplier timestamp) {
      if (preserveTimestamps && ice.getLastUsed() >= 0) {
         return ice.getLastUsed();
      } else {
         return timestamp.getAsLong();
      }
   }


   @Override
   public InternalCacheEntry update(InternalCacheEntry cacheEntry, Object value, Metadata metadata) {
      // Update value and metadata atomically. Any attempt to get a copy of
      // the cache entry should also acquire the same lock, to avoid returning
      // partially applied cache entry updates
      synchronized (cacheEntry) {
         cacheEntry.setValue(value);
         InternalCacheEntry original = cacheEntry;
         cacheEntry = update(cacheEntry, metadata, false);
         // we have the same instance. So we need to reincarnate, if mortal.
         if (cacheEntry.getLifespan() > 0 && original == cacheEntry) {
            cacheEntry.reincarnate(timeService.wallClockTime());
         }
         return cacheEntry;
      }
   }

   @Override
   public CacheEntry copy(CacheEntry cacheEntry) {
      synchronized (cacheEntry) {
         return cacheEntry.clone();
      }
   }

   @Override
   public <K, V> InternalCacheEntry createL1(K key, V value, Metadata metadata) {
      if (!isStoreMetadata(metadata, false)) {
         return new L1InternalCacheEntry(key, value, metadata.lifespan(), timeService.wallClockTime());
      } else {
         return new L1MetadataInternalCacheEntry(key, value, metadata, timeService.wallClockTime());
      }
   }

   @Override
   public InternalCacheValue getValueFromCtxOrCreateNew(Object key, InvocationContext ctx) {
      CacheEntry entry = ctx.lookupEntry(key);
      if (entry instanceof InternalCacheEntry) {
         return ((InternalCacheEntry) entry).toInternalCacheValue(true);
      } else {
         if (ctx.isInTxScope()) {
            EntryVersionsMap updatedVersions =
                  ((TxInvocationContext) ctx).getCacheTransaction().getUpdatedEntryVersions();
            if (updatedVersions != null) {
               EntryVersion version = updatedVersions.get(entry.getKey());
               if (version != null) {
                  Metadata metadata = entry.getMetadata();
                  if (metadata == null) {
                     // If no metadata passed, assumed embedded metadata
                     metadata = new EmbeddedMetadata.Builder()
                           .lifespan(entry.getLifespan()).maxIdle(entry.getMaxIdle())
                           .version(version).build();
                     return create(entry.getKey(), entry.getValue(), metadata)
                           .toInternalCacheValue(true);
                  } else {
                     metadata = metadata.builder().version(version).build();
                     return create(entry.getKey(), entry.getValue(), metadata)
                           .toInternalCacheValue(true);
                  }
               }
            }
         }
         return create(entry).toInternalCacheValue(true);
      }
   }

   /**
    * Indicates whether the entire metadata object needs to be stored or not.
    *
    * This check is done to avoid keeping the entire metadata object around
    * when only lifespan or maxIdle time is stored. If more information
    * needs to be stored (i.e. version), or the metadata object is not the
    * embedded one, keep the entire metadata object around.
    *
    * @return true if the entire metadata object needs to be stored, otherwise
    * simply store lifespan and/or maxIdle in existing cache entries
    */
   public static boolean isStoreMetadata(Metadata metadata, boolean replicable) {
      return metadata != null && (metadata.version() != null || (!replicable && metadata.lastInvocation() != null) || !(metadata instanceof EmbeddedMetadata));
   }
}
