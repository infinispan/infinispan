package org.infinispan.compatibility.adaptor52x;

import org.infinispan.config.Configuration;
import org.infinispan.config.parsing.XmlConfigHelper;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.CacheStore;
import org.infinispan.marshall.StreamingMarshallerAdapter;
import org.infinispan.metadata.InternalMetadataImpl;
import org.infinispan.persistence.MarshalledEntryImpl;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.TaskContextImpl;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshalledEntry;
import org.infinispan.util.Util;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class Adaptor52xStore implements AdvancedLoadWriteStore {

   private InitializationContext ctx;
   private Adaptor52xStoreConfiguration configuration;
   private CacheLoader loader;
   private InternalEntryFactory entryFactory;

   @Override
   public void init(InitializationContext ctx) {
      this.ctx = ctx;
      configuration = ctx.getConfiguration();
      loader = configuration.getLoader();
      CacheLoaderConfig cacheLoaderConfig = instantiateCacheLoaderConfig(loader.getClass());
      XmlConfigHelper.setValues(cacheLoaderConfig, configuration.properties(), false, true);
      try {
         loader.init(cacheLoaderConfig, ctx.getCache(), new StreamingMarshallerAdapter(ctx.getMarshaller()));
      } catch (CacheLoaderException e) {
         throw newCacheLoaderException(e);
      }
   }

   @Override
   public void start() {
      try {
         loader.start();
      } catch (CacheLoaderException e) {
         throw newCacheLoaderException(e);
      }
      entryFactory = ctx.getCache().getAdvancedCache().getComponentRegistry().getComponent(InternalEntryFactory.class);
   }

   @Override
   public void stop() {
      try {
         loader.stop();
      } catch (CacheLoaderException e) {
         throw newCacheLoaderException(e);
      }
   }

   private CacheLoaderConfig instantiateCacheLoaderConfig(Class clazz) {
      // first see if the type is annotated
      Class<? extends CacheLoaderConfig> cacheLoaderConfigType;
      CacheLoaderMetadata metadata = (CacheLoaderMetadata) clazz.getAnnotation(CacheLoaderMetadata.class);
      if (metadata == null) {
         CacheLoader cl = (CacheLoader) Util.getInstance(clazz);
         cacheLoaderConfigType = cl.getConfigurationClass();
      } else {
         cacheLoaderConfigType = metadata.configurationClass();
      }
      return Util.getInstance(cacheLoaderConfigType);
   }


   @Override
   public void process(KeyFilter keyFilter, CacheLoaderTask cacheLoaderTask, Executor executor, boolean fetchValue, boolean fetchMetadata) {
      try {
         Set<Object> keys = loader.loadAllKeys(null);

         int batchSize = 1000;
         ExecutorCompletionService ecs = new ExecutorCompletionService(executor);
         int tasks = 0;
         final TaskContext taskContext = new TaskContextImpl();
         Set<Object> entries = new HashSet<Object>(batchSize);
         for (Object key : keys) {
            if (keyFilter == null || keyFilter.shouldLoadKey(key))
               entries.add(key);
            if (entries.size() == batchSize) {
               final Set<Object> batch = entries;
               entries = new HashSet<Object>(batchSize);
               submitProcessTask(cacheLoaderTask, ecs, taskContext, batch, fetchValue, fetchMetadata);
               tasks++;
            }
         }
         if (!entries.isEmpty()) {
            submitProcessTask(cacheLoaderTask, ecs, taskContext, entries, fetchValue, fetchMetadata);
            tasks++;
         }
         PersistenceUtil.waitForAllTasksToComplete(ecs, tasks);
      } catch (CacheLoaderException e) {
         throw newCacheLoaderException(e);
      }
   }

   private void submitProcessTask(final CacheLoaderTask cacheLoaderTask, ExecutorCompletionService ecs,
                                  final TaskContext taskContext, final Set<Object> batch, final boolean loadEntry,
                                  final boolean loadMetadata) {
      ecs.submit(new Callable<Void>() {
         @Override
         public Void call() throws Exception {
            for (Object key : batch) {
               if (taskContext.isStopped())
                  break;
               if (!loadEntry && !loadMetadata) {
                  cacheLoaderTask.processEntry(new MarshalledEntryImpl(key, (Object) null, null, ctx.getMarshaller()), taskContext);
               } else {
                  cacheLoaderTask.processEntry(load(key), taskContext);
               }
            }
            return null;
         }
      });
   }


   @Override
   public int size() {
      return PersistenceUtil.count(this, null);
   }

   @Override
   public void clear() {
      if (loader instanceof CacheStore)
         try {
            ((CacheStore) loader).clear();
         } catch (CacheLoaderException e) {
            throw newCacheLoaderException(e);
         }
   }

   @Override
   public void purge(Executor threadPool, PurgeListener listener) {
      if (loader instanceof CacheStore)
         try {
            ((CacheStore) loader).purgeExpired();
         } catch (CacheLoaderException e) {
            throw newCacheLoaderException(e);
         }
   }

   @Override
   public MarshalledEntry load(Object key) {
      try {
         InternalCacheEntry load = loader.load(key);
         if (load == null)
            return null;
         return new MarshalledEntryImpl(key, load.getValue(), new InternalMetadataImpl(load), ctx.getMarshaller());
      } catch (CacheLoaderException e) {
         throw newCacheLoaderException(e);
      }
   }

   @Override
   public boolean contains(Object key) {
      return load(key) != null;
   }

   @Override
   public void write(MarshalledEntry entry) {
      if (loader instanceof CacheStore)
         try {
            ((CacheStore) loader).store(entryFactory.create(entry.getKey(), entry.getValue(), entry.getMetadata()));
         } catch (CacheLoaderException e) {
            throw newCacheLoaderException(e);
         }
   }

   @Override
   public boolean delete(Object key) {
      if (loader instanceof CacheStore)
         try {
            return ((CacheStore) loader).remove(key);
         } catch (CacheLoaderException e) {
            throw newCacheLoaderException(e);
         }
      return false;
   }

   private org.infinispan.persistence.CacheLoaderException newCacheLoaderException(CacheLoaderException e) {
      return new org.infinispan.persistence.CacheLoaderException(e);
   }

   public CacheLoader getLoader() {
      return loader;
   }
}
