package org.infinispan.persistence.remote.upgrade;

import static java.util.Spliterators.spliteratorUnknownSize;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.InvocationHelper;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commons.dataconversion.ByteArrayWrapper;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.threads.BlockingThreadFactory;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.persistence.remote.logging.Log;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.logging.LogFactory;

@ProtoTypeId(ProtoStreamTypeIds.REMOTE_STORE_MIGRATION_TASK)
public class MigrationTask implements Function<EmbeddedCacheManager, Integer> {

   private static final Log log = LogFactory.getLog(MigrationTask.class, Log.class);

   private static final String THREAD_NAME = "RollingUpgrade-MigrationTask";

   @ProtoField(1)
   final String cacheName;

   @ProtoField(value = 2, collectionImplementation = HashSet.class)
   final Set<Integer> segments;

   @ProtoField(value = 3, defaultValue = "-1")
   final int readBatch;

   @ProtoField(value = 4, defaultValue = "-1")
   final int threads;

   private final Set<Object> deletedKeys = ConcurrentHashMap.newKeySet();
   private InvocationHelper invocationHelper;
   private CommandsFactory commandsFactory;
   private KeyPartitioner keyPartitioner;

   @ProtoFactory
   public MigrationTask(String cacheName, Set<Integer> segments, int readBatch, int threads) {
      this.cacheName = cacheName;
      this.segments = segments;
      this.readBatch = readBatch;
      this.threads = threads;
   }

   @Override
   public Integer apply(EmbeddedCacheManager embeddedCacheManager) {
      AtomicInteger counter = new AtomicInteger(0);
      DefaultThreadFactory threadFactory = new BlockingThreadFactory(1, THREAD_NAME + "-%t", null, null);
      ExecutorService executorService = Executors.newFixedThreadPool(threads, threadFactory);
      RemoveListener listener = null;
      AdvancedCache<Object, Object> advancedCache = embeddedCacheManager.getCache(cacheName).getAdvancedCache();
      Cache cache = advancedCache.withStorageMediaType();
      try {
         ComponentRegistry cr =  ComponentRegistry.of(cache);
         invocationHelper = cr.getComponent(InvocationHelper.class);
         commandsFactory = cr.getCommandsFactory();
         keyPartitioner = cr.getComponent(KeyPartitioner.class);
         PersistenceManager loaderManager = cr.getComponent(PersistenceManager.class);
         Set<RemoteStore<Object, Object>> stores = (Set) loaderManager.getStores(RemoteStore.class);
         listener = new RemoveListener();
         cache.addFilteredListener(listener, new RemovedFilter<>(), null, Util.asSet(CacheEntryRemoved.class));

         Iterator<RemoteStore<Object, Object>> storeIterator = stores.iterator();
         if (storeIterator.hasNext()) {
            RemoteStore<Object, Object> store = storeIterator.next();
            RemoteCache<Object, Object> storeCache = store.getRemoteCache();
            migrateEntriesWithMetadata(storeCache, counter, executorService, cache);
         }
      } finally {
         if (listener != null) {
            cache.removeListener(listener);
         }
         executorService.shutdown();
      }

      return counter.get();
   }

   @Listener(clustered = true)
   @SuppressWarnings("unused")
   private class RemoveListener {
      @CacheEntryRemoved
      public void entryRemoved(CacheEntryRemovedEvent event) {
         deletedKeys.add(ByteArrayWrapper.INSTANCE.wrap(event.getKey()));
      }
   }

   private void migrateEntriesWithMetadata(RemoteCache<Object, Object> sourceCache, AtomicInteger counter,
                                           ExecutorService executorService, Cache<Object, Object> cache) {
      AdvancedCache<Object, Object> destinationCache = cache.getAdvancedCache();
      DataConversion keyDataConversion = destinationCache.getKeyDataConversion();
      DataConversion valueDataConversion = destinationCache.getValueDataConversion();
      try (CloseableIterator<Entry<Object, MetadataValue<Object>>> iterator = sourceCache.retrieveEntriesWithMetadata(segments, readBatch)) {
         CompletableFuture<?>[] completableFutures = StreamSupport.stream(spliteratorUnknownSize(iterator, 0), false)
               .map(entry -> {
                  Object key = entry.getKey();
                  MetadataValue<Object> metadataValue = entry.getValue();
                  int lifespan = metadataValue.getLifespan();
                  int maxIdle = metadataValue.getMaxIdle();
                  long version = metadataValue.getVersion();
                  Metadata metadata = new EmbeddedMetadata.Builder()
                        .version(new NumericVersion(version))
                        .lifespan(lifespan, TimeUnit.SECONDS)
                        .maxIdle(maxIdle, TimeUnit.SECONDS)
                        .build();
                  if (!deletedKeys.contains(ByteArrayWrapper.INSTANCE.wrap(key))) {
                     return CompletableFuture.supplyAsync(() -> {
                        int currentCount = counter.incrementAndGet();
                        if (log.isDebugEnabled() && currentCount % 100 == 0)
                           log.debugf(">>    Migrated %s entries\n", currentCount);
                        return writeToDestinationCache(entry, metadata, keyDataConversion, valueDataConversion);
                     }, executorService);
                  }
                  return CompletableFuture.completedFuture(null);
               }).toArray(CompletableFuture[]::new);

         CompletableFuture.allOf(completableFutures).join();
      }
   }

   private Object writeToDestinationCache(Entry<Object, MetadataValue<Object>> entry, Metadata metadata, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      Object key = keyDataConversion.toStorage(entry.getKey());
      Object value = valueDataConversion.toStorage(entry.getValue().getValue());

      int segment = keyPartitioner.getSegment(key);
      long flags = EnumUtil.bitSetOf(Flag.SKIP_CACHE_LOAD, Flag.ROLLING_UPGRADE);
      ComputeCommand computeCommand = commandsFactory.buildComputeCommand(key, new EntryWriter<>(value), false, segment, metadata, flags);
      InvocationContext context = invocationHelper.createInvocationContextWithImplicitTransaction(1, true);
      return invocationHelper.invoke(context, computeCommand);
   }

   @ProtoTypeId(ProtoStreamTypeIds.REMOTE_STORE_MIGRATION_TASK_ENTRY_WRITER)
   public static class EntryWriter<K, V> implements BiFunction<K, V, V> {
      private final V newEntry;

      public EntryWriter(V newEntry) {
         this.newEntry = newEntry;
      }

      @ProtoFactory
      EntryWriter(MarshallableObject<V> wrappedNewEntry) {
         this.newEntry = MarshallableObject.unwrap(wrappedNewEntry);
      }

      @ProtoField(1)
      MarshallableObject<V> getWrappedNewEntry() {
         return MarshallableObject.create(newEntry);
      }

      @Override
      public V apply(K k, V v) {
         return v == null ? newEntry : v;
      }
   }
}
