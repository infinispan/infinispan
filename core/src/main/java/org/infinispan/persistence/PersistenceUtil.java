package org.infinispan.persistence;

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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class PersistenceUtil {

   private static Log log = LogFactory.getLog(PersistenceUtil.class);

   public static KeyFilter notNull(KeyFilter filter) {
      return filter == null ? KeyFilter.LOAD_ALL_FILTER : filter;
   }

   public static <K, V> int count(AdvancedCacheLoader<K, V> acl, KeyFilter<? super K> filter) {
      final AtomicInteger result = new AtomicInteger(0);
      acl.process(filter, new AdvancedCacheLoader.CacheLoaderTask<K, V>() {
         @Override
         public void processEntry(MarshalledEntry<K, V> marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
            result.incrementAndGet();
         }
      }, new WithinThreadExecutor(), false, false);
      return result.get();
   }

   public static <K, V> Set<K> toKeySet(AdvancedCacheLoader<K, V> acl, KeyFilter<? super K> filter) {
      if (acl == null)
         return Collections.emptySet();
      final Set<K> set = new HashSet<K>();
      acl.process(filter, new AdvancedCacheLoader.CacheLoaderTask<K, V>() {
         @Override
         public void processEntry(MarshalledEntry<K, V> marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
            set.add(marshalledEntry.getKey());
         }
      }, new WithinThreadExecutor(), false, false);
      return set;
   }

   public static <K, V> Set<InternalCacheEntry> toEntrySet(AdvancedCacheLoader<K, V> acl, KeyFilter<? super K> filter, final InternalEntryFactory ief) {
      if (acl == null)
         return Collections.emptySet();
      final Set<InternalCacheEntry> set = new HashSet<InternalCacheEntry>();
      acl.process(filter, new AdvancedCacheLoader.CacheLoaderTask<K, V>() {
         @Override
         public void processEntry(MarshalledEntry<K, V> ce, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
            set.add(ief.create(ce.getKey(), ce.getValue(), ce.getMetadata()));
         }
      }, new WithinThreadExecutor(), true, true);
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
      return dataContainer.compute(key, new DataContainer.ComputeAction<K, V>() {
         @Override
         public InternalCacheEntry<K, V> compute(K key, InternalCacheEntry<K, V> oldEntry,
                                                 InternalEntryFactory factory) {
            //under the lock, check if the entry exists in the DataContainer
            if (oldEntry != null) {
               isLoaded.set(null); //not loaded
               return oldEntry; //no changes in container
            }

            MarshalledEntry loaded = loadAndCheckExpiration(persistenceManager, key, ctx, timeService);
            if (loaded == null) {
               isLoaded.set(Boolean.FALSE); //not loaded
               return null; //no changed in container
            }

            InternalCacheEntry<K, V> newEntry = convert(loaded, factory);

            isLoaded.set(Boolean.TRUE); //loaded!
            return newEntry;
         }
      });
   }

   public static MarshalledEntry loadAndCheckExpiration(PersistenceManager persistenceManager, Object key,
                                                        InvocationContext context, TimeService timeService) {
      final MarshalledEntry loaded = persistenceManager.loadFromAllStores(key, context);
      if (log.isTraceEnabled()) {
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
