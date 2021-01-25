package org.infinispan.persistence.remote.upgrade;

import static java.util.Spliterators.spliteratorUnknownSize;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.BitSet;
import java.util.Collections;
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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.InvocationHelper;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commons.dataconversion.ByteArrayWrapper;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
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
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.remote.ExternalizerIds;
import org.infinispan.persistence.remote.RemoteStore;
import org.infinispan.persistence.remote.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class MigrationTask implements Function<EmbeddedCacheManager, Integer> {

   private static final Log log = LogFactory.getLog(MigrationTask.class, Log.class);

   private static final String THREAD_NAME = "RollingUpgrade-MigrationTask";

   private final String cacheName;
   private final Set<Integer> segments;
   private final int readBatch;
   private final int threads;
   private final Set<Object> deletedKeys = ConcurrentHashMap.newKeySet();

   private InvocationHelper invocationHelper;
   private CommandsFactory commandsFactory;
   private KeyPartitioner keyPartitioner;

   public MigrationTask(String cacheName, Set<Integer> segments, int readBatch, int threads) {
      this.cacheName = cacheName;
      this.segments = segments;
      this.readBatch = readBatch;
      this.threads = threads;
   }

   @Override
   public Integer apply(EmbeddedCacheManager embeddedCacheManager) {
      AtomicInteger counter = new AtomicInteger(0);
      DefaultThreadFactory threadFactory = new BlockingThreadFactory(null, 1, THREAD_NAME + "-%t", null, null);
      ExecutorService executorService = Executors.newFixedThreadPool(threads, threadFactory);
      RemoveListener listener = null;
      AdvancedCache<Object, Object> advancedCache = embeddedCacheManager.getCache(cacheName).getAdvancedCache();
      Cache cache = advancedCache.withStorageMediaType();
      try {
         ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
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

   public static class Externalizer extends AbstractExternalizer<MigrationTask> {

      @Override
      public Set<Class<? extends MigrationTask>> getTypeClasses() {
         return Collections.singleton(MigrationTask.class);
      }

      @Override
      public void writeObject(ObjectOutput output, MigrationTask task) throws IOException {
         output.writeObject(task.cacheName);
         UnsignedNumeric.writeUnsignedInt(output, task.readBatch);
         UnsignedNumeric.writeUnsignedInt(output, task.threads);
         BitSet bs = new BitSet();
         for (Integer segment : task.segments) {
            bs.set(segment);
         }
         byte[] bytes = bs.toByteArray();
         UnsignedNumeric.writeUnsignedInt(output, bytes.length);
         output.write(bs.toByteArray());
      }

      @Override
      public MigrationTask readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String cacheName = (String) input.readObject();
         int readBatch = UnsignedNumeric.readUnsignedInt(input);
         int threads = UnsignedNumeric.readUnsignedInt(input);
         int segmentsSize = UnsignedNumeric.readUnsignedInt(input);
         byte[] bytes = new byte[segmentsSize];
         input.read(bytes);
         BitSet bitSet = BitSet.valueOf(bytes);
         Set<Integer> segments = bitSet.stream().boxed().collect(Collectors.toSet());
         return new MigrationTask(cacheName, segments, readBatch, threads);
      }
   }


   public static class EntryWriter<K, V> implements BiFunction<K, V, V> {
      private final V newEntry;

      public EntryWriter(V newEntry) {
         this.newEntry = newEntry;
      }

      @Override
      public V apply(K k, V v) {
         return v == null ? newEntry : v;
      }
   }

   public static class EntryWriterExternalizer extends AbstractExternalizer<EntryWriter> {

      @Override
      public Set<Class<? extends EntryWriter>> getTypeClasses() {
         return Collections.singleton(EntryWriter.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.ENTRY_WRITER;
      }

      @Override
      public void writeObject(ObjectOutput output, EntryWriter object) throws IOException {
         output.writeObject(object.newEntry);
      }

      @Override
      public EntryWriter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object newEntry = input.readObject();
         return new EntryWriter(newEntry);
      }
   }
}
