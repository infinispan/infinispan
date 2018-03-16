package org.infinispan.persistence;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.commons.util.ByRef;
import org.infinispan.container.DataContainer;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.InvocationContext;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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

   public static <K, V> int count(AdvancedCacheLoader<K, V> acl, KeyFilter<? super K> filter) {
      final AtomicInteger result = new AtomicInteger(0);
      acl.process(filter, (marshalledEntry, taskContext) -> result.incrementAndGet(), new WithinThreadExecutor(), false, false);
      return result.get();
   }

   public static <K, V> Set<K> toKeySet(AdvancedCacheLoader<K, V> acl, KeyFilter<? super K> filter) {
      if (acl == null)
         return Collections.emptySet();
      final Set<K> set = new HashSet<K>();
      acl.process(filter, (marshalledEntry, taskContext) -> set.add(marshalledEntry.getKey()), new WithinThreadExecutor(), false, false);
      return set;
   }

   public static <K, V> Set<InternalCacheEntry> toEntrySet(AdvancedCacheLoader<K, V> acl, KeyFilter<? super K> filter, final InternalEntryFactory ief) {
      if (acl == null)
         return Collections.emptySet();
      final Set<InternalCacheEntry> set = new HashSet<InternalCacheEntry>();
      acl.process(filter, (ce, taskContext) -> set.add(ief.create(ce.getKey(), ce.getValue(), ce.getMetadata())), new WithinThreadExecutor(), true, true);
      return set;
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

   public static <K, V> InternalCacheEntry<K,V> loadAndStoreInDataContainer(DataContainer<K, V> dataContainer, final PersistenceManager persistenceManager,
                                                         K key, final InvocationContext ctx, final TimeService timeService,
                                                         final AtomicReference<Boolean> isLoaded) {
      final ByRef<Boolean> expired = new ByRef<>(null);
      InternalCacheEntry<K,V> entry = dataContainer.compute(key, (k, oldEntry, factory) -> {
         //under the lock, check if the entry exists in the DataContainer
         if (oldEntry != null) {
            if (isLoaded != null) {
               isLoaded.set(null); //not loaded
            }
            if (oldEntry.canExpire() && oldEntry.isExpired(timeService.wallClockTime())) {
               expired.set(Boolean.TRUE);
               return oldEntry;
            }
            return oldEntry; //no changes in container
         }

         // Load using key from command
         MarshalledEntry<K, V> loaded = loadAndCheckExpiration(persistenceManager, key, ctx, timeService);
         if (loaded == null) {
            if (isLoaded != null) {
               isLoaded.set(Boolean.FALSE); //not loaded
            }
            return null; //no changed in container
         }

         InternalCacheEntry<K, V> newEntry = convert(loaded, factory);

         if (isLoaded != null) {
            isLoaded.set(Boolean.TRUE); //loaded!
         }
         return newEntry;
      });
      if (expired.get() == Boolean.TRUE) {
         return null;
      } else {
         return entry;
      }
   }

   public static <K, V> InternalCacheEntry<K,V> loadAndComputeInDataContainer(DataContainer<K, V> dataContainer, final PersistenceManager persistenceManager,
                                                                              K key, final InvocationContext ctx, final TimeService timeService,
                                                                              DataContainer.ComputeAction<K, V> action) {
      final ByRef<Boolean> expired = new ByRef<>(null);
      InternalCacheEntry<K,V> entry = dataContainer.compute(key, (k, oldEntry, factory) -> {
         //under the lock, check if the entry exists in the DataContainer
         if (oldEntry != null) {
            if (oldEntry.canExpire() && oldEntry.isExpired(timeService.wallClockTime())) {
               expired.set(Boolean.TRUE);
               return oldEntry;
            }
            return action.compute(k, oldEntry, factory);
         }

         // Load using key from command
         MarshalledEntry<K, V> loaded = loadAndCheckExpiration(persistenceManager, key, ctx, timeService);
         if (loaded == null) {
            return action.compute(k, null, factory);
         }

         InternalCacheEntry<K, V> newEntry = convert(loaded, factory);
         return action.compute(k, newEntry, factory);
      });
      if (expired.get() == Boolean.TRUE) {
         return null;
      } else {
         return entry;
      }
   }

   public static <K, V> MarshalledEntry<K, V> loadAndCheckExpiration(PersistenceManager persistenceManager, K key,
                                                        InvocationContext context, TimeService timeService) {
      final MarshalledEntry<K, V> loaded = persistenceManager.loadFromAllStores(key, context);
      if (trace) {
         log.tracef("Loaded %s for key %s from persistence.", loaded, key);
      }
      if (loaded == null) {
         return null;
      }
      InternalMetadata metadata = loaded.getMetadata();
      if (metadata != null && metadata.isExpired(timeService.wallClockTime())) {
         return null;
      }
      return loaded;
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
