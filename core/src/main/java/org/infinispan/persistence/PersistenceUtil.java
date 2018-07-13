package org.infinispan.persistence;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.infinispan.commons.util.ByRef;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.Flowable;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class PersistenceUtil {

   private static Log log = LogFactory.getLog(PersistenceUtil.class);
   private static final boolean trace = log.isTraceEnabled();

   public static KeyFilter notNull(KeyFilter filter) {
      return filter == null ? KeyFilter.ACCEPT_ALL_FILTER : filter;
   }

   private static final int SEGMENT_NOT_PROVIDED = -1;

   /**
    *
    * @param acl
    * @param filter
    * @param <K>
    * @param <V>
    * @return
    * @deprecated Please use {@link #count(AdvancedCacheLoader, Predicate)} instead
    */
   @Deprecated
   public static <K, V> int count(AdvancedCacheLoader<K, V> acl, KeyFilter<? super K> filter) {
      return count(acl, (Predicate<? super K>) filter);
   }

   public static <K, V> int count(AdvancedCacheLoader<K, V> acl, Predicate<? super K> filter) {

      // This can't be null
      Long result = Flowable.fromPublisher(acl.publishKeys(filter)).count().blockingGet();
      if (result > Integer.MAX_VALUE) {
         return Integer.MAX_VALUE;
      }
      return result.intValue();
   }

   /**
    *
    * @param acl
    * @param filter
    * @param <K>
    * @param <V>
    * @return
    * @deprecated Please use {@link #toKeySet(AdvancedCacheLoader, Predicate)} instead
    */
   @Deprecated
   public static <K, V> Set<K> toKeySet(AdvancedCacheLoader<K, V> acl, KeyFilter<? super K> filter) {
      return toKeySet(acl, (Predicate<? super K>) filter);
   }

   public static <K, V> Set<K> toKeySet(AdvancedCacheLoader<K, V> acl, Predicate<? super K> filter) {
      if (acl == null)
         return Collections.emptySet();
      return Flowable.fromPublisher(acl.publishKeys(filter))
            .collectInto(new HashSet<K>(), Set::add).blockingGet();
   }

   /**
    *
    * @param acl
    * @param filter
    * @param ief
    * @param <K>
    * @param <V>
    * @return
    * @deprecated Please use {@link #toEntrySet(AdvancedCacheLoader, Predicate, InternalEntryFactory)} instead
    */
   @Deprecated
   public static <K, V> Set<InternalCacheEntry> toEntrySet(AdvancedCacheLoader<K, V> acl, KeyFilter<? super K> filter, final InternalEntryFactory ief) {
      Set entrySet = toEntrySet(acl, (Predicate<? super K>) filter, ief);
      return (Set<InternalCacheEntry>) entrySet;
   }

   public static <K, V> Set<InternalCacheEntry<K, V>> toEntrySet(AdvancedCacheLoader<K, V> acl, Predicate<? super K> filter, final InternalEntryFactory ief) {
      if (acl == null)
         return Collections.emptySet();
      return Flowable.fromPublisher(acl.publishEntries(filter, true, true))
            .map(me -> ief.create(me.getKey(), me.getValue(), me.getMetadata()))
            .collectInto(new HashSet<InternalCacheEntry<K, V>>(), Set::add).blockingGet();
   }

   public static long getExpiryTime(InternalMetadata internalMetadata) {
      return internalMetadata == null ? -1 : internalMetadata.expiryTime();
   }

   public static InternalMetadata internalMetadata(InternalCacheEntry ice) {
      return ice.getMetadata() == null ? null : new InternalMetadataImpl(ice);
   }

   public static InternalMetadata internalMetadata(InternalCacheValue icv) {
      return icv.getMetadata() == null ? null : new InternalMetadataImpl(icv.getMetadata(), icv.getCreated(), icv.getLastUsed());
   }

   public static <K, V> InternalCacheEntry<K,V> loadAndStoreInDataContainer(DataContainer<K, V> dataContainer,
         final PersistenceManager persistenceManager, K key, final InvocationContext ctx, final TimeService timeService,
         final AtomicReference<Boolean> isLoaded) {
      return loadAndStoreInDataContainer(dataContainer, SEGMENT_NOT_PROVIDED, persistenceManager, key, ctx, timeService,
            isLoaded);
   }

   private static <K, V> InternalCacheEntry<K, V> loadAndComputeInDataContainer(DataContainer<K, V> dataContainer,
         int segment, final PersistenceManager persistenceManager, K key, final InvocationContext ctx,
         final TimeService timeService, DataContainer.ComputeAction<K, V> action, final AtomicReference<Boolean> isLoaded) {
      final ByRef<Boolean> expired = new ByRef<>(null);

      DataContainer.ComputeAction<K, V> computeAction = (k, oldEntry, factory) -> {
         InternalCacheEntry<K, V> entryToUse;
         //under the lock, check if the entry exists in the DataContainer
         if (oldEntry != null) {
            if (oldEntry.canExpire() && oldEntry.isExpired(timeService.wallClockTime())) {
               // If it was expired we can check CacheLoaders - since they can have different
               // metadata than a store
               MarshalledEntry<K, V> loaded = loadAndCheckExpiration(persistenceManager, key, ctx, false);
               if (loaded != null) {
                  if (isLoaded != null) {
                     isLoaded.set(Boolean.TRUE); //loaded!
                  }
                  entryToUse = convert(loaded, factory);

               } else {
                  if (isLoaded != null) {
                     isLoaded.set(Boolean.FALSE); //not loaded
                  }
                  expired.set(Boolean.TRUE);
                  // Return the original entry - so it doesn't remove expired entry early
                  return oldEntry;
               }
            } else {
               if (isLoaded != null) {
                  isLoaded.set(null); //no attempt to load
               }
               entryToUse = oldEntry;
            }
         } else {
            // There was no entry in memory so check all the stores to see if it is there
            MarshalledEntry<K, V> loaded = loadAndCheckExpiration(persistenceManager, key, ctx, true);
            if (loaded != null) {
               if (isLoaded != null) {
                  isLoaded.set(Boolean.TRUE); //loaded!
               }
               entryToUse = convert(loaded, factory);
            } else {if (isLoaded != null) {
               isLoaded.set(Boolean.FALSE); //not loaded
            }

               entryToUse = null;
            }
         }

         if (action != null) {
            return action.compute(k, entryToUse, factory);
         } else {
            return entryToUse;
         }
      };
      InternalCacheEntry<K,V> entry;
      if (segment != SEGMENT_NOT_PROVIDED && dataContainer instanceof InternalDataContainer) {
         entry = ((InternalDataContainer<K, V>) dataContainer).compute(segment, key, computeAction);
      } else {
         entry = dataContainer.compute(key, computeAction);
      }
      if (expired.get() == Boolean.TRUE) {
         return null;
      } else {
         return entry;
      }
   }

   public static <K, V> InternalCacheEntry<K,V> loadAndStoreInDataContainer(DataContainer<K, V> dataContainer, int segment,
         final PersistenceManager persistenceManager, K key, final InvocationContext ctx, final TimeService timeService,
                                                         final AtomicReference<Boolean> isLoaded) {
      return loadAndComputeInDataContainer(dataContainer, segment, persistenceManager, key, ctx, timeService, null, isLoaded);
   }

   public static <K, V> InternalCacheEntry<K,V> loadAndComputeInDataContainer(DataContainer<K, V> dataContainer,
         int segment, final PersistenceManager persistenceManager, K key, final InvocationContext ctx,
         final TimeService timeService, DataContainer.ComputeAction<K, V> action) {
      return loadAndComputeInDataContainer(dataContainer, segment, persistenceManager, key, ctx, timeService, action, null);
   }

   private static <K, V> MarshalledEntry<K, V> loadAndCheckExpiration(PersistenceManager persistenceManager, Object key,
         InvocationContext context, boolean includeStores) {
      final MarshalledEntry<K, V> loaded = persistenceManager.loadFromAllStores(key, context.isOriginLocal(),
            includeStores);
      if (trace) {
         log.tracef("Loaded %s for key %s from persistence.", loaded, key);
      }
      return loaded;
   }

   public static <K, V> MarshalledEntry<K, V> loadAndCheckExpiration(PersistenceManager persistenceManager, Object key,
                                                        InvocationContext context, TimeService timeService) {
      return loadAndCheckExpiration(persistenceManager, key, context, true);
   }

   public static <K, V> InternalCacheEntry<K, V> convert(MarshalledEntry<K, V> loaded, InternalEntryFactory factory) {
      InternalMetadata metadata = loaded.getMetadata();
      if (metadata != null) {
         Metadata actual = metadata instanceof InternalMetadataImpl ? ((InternalMetadataImpl) metadata).actual() :
               metadata;
         //noinspection unchecked
         return factory.create(loaded.getKey(), loaded.getValue(), actual, metadata.created(), metadata.lifespan(),
                               metadata.lastUsed(), metadata.maxIdle());
      } else {
         //metadata is null!
         //noinspection unchecked
         return factory.create(loaded.getKey(), loaded.getValue(), (Metadata) null);
      }
   }
}
