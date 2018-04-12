package org.infinispan.persistence.manager;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;
import static org.infinispan.context.Flag.IGNORE_RETURN_VALUES;
import static org.infinispan.context.Flag.SKIP_CACHE_STORE;
import static org.infinispan.context.Flag.SKIP_INDEXING;
import static org.infinispan.context.Flag.SKIP_LOCKING;
import static org.infinispan.context.Flag.SKIP_OWNERSHIP_CHECK;
import static org.infinispan.context.Flag.SKIP_XSITE_BACKUP;
import static org.infinispan.factories.KnownComponentNames.PERSISTENCE_EXECUTOR;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.context.Flag;
import org.infinispan.eviction.EvictionType;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.filter.KeyFilter;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.CacheLoaderInterceptor;
import org.infinispan.interceptors.impl.CacheWriterInterceptor;
import org.infinispan.interceptors.impl.TransactionalStoreInterceptor;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.persistence.InitializationContextImpl;
import org.infinispan.persistence.async.AdvancedAsyncCacheLoader;
import org.infinispan.persistence.async.AdvancedAsyncCacheWriter;
import org.infinispan.persistence.async.AsyncCacheLoader;
import org.infinispan.persistence.async.AsyncCacheWriter;
import org.infinispan.persistence.async.State;
import org.infinispan.persistence.factory.CacheStoreFactoryRegistry;
import org.infinispan.persistence.spi.AdvancedCacheExpirationWriter;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.AdvancedCacheWriter;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.CacheWriter;
import org.infinispan.persistence.spi.FlagAffectedStore;
import org.infinispan.persistence.spi.LocalOnlyCacheLoader;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.spi.TransactionalCacheWriter;
import org.infinispan.persistence.support.AdvancedSingletonCacheWriter;
import org.infinispan.persistence.support.BatchModification;
import org.infinispan.persistence.support.DelegatingCacheLoader;
import org.infinispan.persistence.support.DelegatingCacheWriter;
import org.infinispan.persistence.support.SingletonCacheWriter;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.GuardedBy;

public class PersistenceManagerImpl implements PersistenceManager {

   private static final Log log = LogFactory.getLog(PersistenceManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject private Configuration configuration;
   @Inject private AdvancedCache<Object, Object> cache;
   @Inject private StreamingMarshaller m;
   @Inject private TransactionManager transactionManager;
   @Inject private TimeService timeService;
   @Inject @ComponentName(PERSISTENCE_EXECUTOR)
   private Executor persistenceExecutor;
   @Inject private ByteBufferFactory byteBufferFactory;
   @Inject private MarshalledEntryFactory marshalledEntryFactory;
   @Inject private CacheStoreFactoryRegistry cacheStoreFactoryRegistry;
   @Inject private ExpirationManager<Object, Object> expirationManager;

   @GuardedBy("storesMutex")
   private final List<CacheLoader> loaders = new ArrayList<>();
   @GuardedBy("storesMutex")
   private final List<CacheWriter> nonTxWriters = new ArrayList<>();
   @GuardedBy("storesMutex")
   private final List<TransactionalCacheWriter> txWriters = new ArrayList<>();
   private final ReadWriteLock storesMutex = new ReentrantReadWriteLock();
   private final Map<Object, StoreConfiguration> configMap = new HashMap<>();
   private AdvancedPurgeListener<Object, Object> advancedListener;

   /**
    * making it volatile as it might change after @Start, so it needs the visibility.
    */
   private volatile boolean enabled;
   private volatile boolean clearOnStop;
   private boolean preloaded;

   @Override
   @Start()
   public void start() {
      advancedListener = new AdvancedPurgeListener<>(expirationManager);
      preloaded = false;
      enabled = configuration.persistence().usingStores();
      if (!enabled)
         return;
      try {
         createLoadersAndWriters();
         Transaction xaTx = null;
         if (transactionManager != null) {
            xaTx = transactionManager.suspend();
         }
         storesMutex.readLock().lock();
         try {

            Set<Lifecycle> undelegated = new HashSet<>();//black magic to make sure the store start only gets invoked once

            Consumer<CacheWriter> startWriter = writer -> {
               writer.start();
               if (writer instanceof DelegatingCacheWriter) {
                  CacheWriter actual = undelegate(writer);
                  actual.start();
                  undelegated.add(actual);
               } else {
                  undelegated.add(writer);
               }

               if (configMap.get(writer).purgeOnStartup()) {
                  if (!(writer instanceof AdvancedCacheWriter))
                     throw new PersistenceException("'purgeOnStartup' can only be set on stores implementing " +
                                                          "" + AdvancedCacheWriter.class.getName());
                  ((AdvancedCacheWriter) writer).clear();
               }
            };
            nonTxWriters.forEach(startWriter);
            txWriters.forEach(startWriter);

            for (CacheLoader l : loaders) {
               if (!undelegated.contains(l))
                  l.start();
               if (l instanceof DelegatingCacheLoader) {
                  CacheLoader actual = undelegate(l);
                  if (!undelegated.contains(actual)) {
                     actual.start();
                  }
               }
            }
         } finally {
            if (xaTx != null) {
               transactionManager.resume(xaTx);
            }
            storesMutex.readLock().unlock();
         }
      } catch (Exception e) {
         throw new CacheException("Unable to start cache loaders", e);
      }
   }

   @Override
   @Stop
   public void stop() {
      storesMutex.writeLock().lock();
      try {
         // If needed, clear the persistent store before stopping
         if (clearOnStop) {
            clearAllStores(AccessMode.BOTH);
         }

         Set<Lifecycle> undelegated = new HashSet<>();
         Consumer<CacheWriter> stopWriters = writer -> {
            writer.stop();
            if (writer instanceof DelegatingCacheWriter) {
               CacheWriter actual = undelegate(writer);
               actual.stop();
               undelegated.add(actual);
            } else {
               undelegated.add(writer);
            }
         };
         nonTxWriters.forEach(stopWriters);
         nonTxWriters.clear();
         txWriters.forEach(stopWriters);
         txWriters.clear();

         for (CacheLoader l : loaders) {
            if (!undelegated.contains(l)) {
               l.stop();
            }
            if (l instanceof DelegatingCacheLoader) {
               CacheLoader actual = undelegate(l);
               if (!undelegated.contains(actual)) {
                  actual.stop();
               }
            }
         }
         loaders.clear();
         preloaded = false;
      } finally {
         storesMutex.writeLock().unlock();
      }
   }

   @Override
   public boolean isEnabled() {
      return enabled;
   }

   @Override
   public boolean isPreloaded() {
      return preloaded;
   }

   @Override
   @Start(priority = 56)
   public void preload() {
      if (!enabled)
         return;
      AdvancedCacheLoader<?, ?> preloadCl = null;

      storesMutex.readLock().lock();
      try {
         for (CacheLoader l : loaders) {
            if (configMap.get(l).preload()) {
               if (!(l instanceof AdvancedCacheLoader)) {
                  throw new PersistenceException("Cannot preload from cache loader '" + l.getClass().getName()
                        + "' as it doesn't implement '" + AdvancedCacheLoader.class.getName() + "'");
               }
               preloadCl = (AdvancedCacheLoader) l;
               if (preloadCl instanceof AdvancedAsyncCacheLoader)
                  preloadCl = (AdvancedCacheLoader) ((AdvancedAsyncCacheLoader) preloadCl).undelegate();
               break;
            }
         }
      } finally {
         storesMutex.readLock().unlock();
      }
      if (preloadCl == null)
         return;

      long start = timeService.time();


      final long maxEntries = getMaxEntries();
      final AtomicInteger loadedEntries = new AtomicInteger(0);
      final AdvancedCache<Object, Object> flaggedCache = getCacheForStateInsertion();
      ByRef.Boolean preloaded = new ByRef.Boolean(true);
      preloadCl.process(null, (me, taskContext) -> {
         if (loadedEntries.getAndIncrement() >= maxEntries) {
            taskContext.stop();
            preloaded.set(false);
            return;
         }
         Metadata metadata = me.getMetadata() != null ? ((InternalMetadataImpl) me.getMetadata()).actual() :
               null; //the downcast will go away with ISPN-3460
         preloadKey(flaggedCache, me.getKey(), me.getValue(), metadata);
      }, new WithinThreadExecutor(), true, true);
      this.preloaded = preloaded.get();

      log.debugf("Preloaded %d keys in %s", loadedEntries.get(), Util.prettyPrintTime(timeService.timeDuration(start, MILLISECONDS)));
   }

   @Override
   public void disableStore(String storeType) {
      if (enabled) {
         boolean noMoreStores;
         storesMutex.writeLock().lock();
         try {
            removeCacheLoader(storeType, loaders);
            removeCacheWriter(storeType, nonTxWriters);
            removeCacheWriter(storeType, txWriters);
            noMoreStores = loaders.isEmpty() && nonTxWriters.isEmpty() && txWriters.isEmpty();
         } finally {
            storesMutex.writeLock().unlock();
         }

         if (noMoreStores) {
            AsyncInterceptorChain chain = cache.getAdvancedCache().getAsyncInterceptorChain();
            AsyncInterceptor loaderInterceptor = chain.findInterceptorExtending(CacheLoaderInterceptor.class);
            if (loaderInterceptor == null) {
               log.persistenceWithoutCacheLoaderInterceptor();
            } else {
               chain.removeInterceptor(loaderInterceptor.getClass());
            }
            AsyncInterceptor writerInterceptor = chain.findInterceptorExtending(CacheWriterInterceptor.class);
            if (writerInterceptor == null) {
               writerInterceptor = chain.findInterceptorWithClass(TransactionalStoreInterceptor.class);
               if (writerInterceptor == null) {
                  log.persistenceWithoutCacheWriteInterceptor();
               } else {
                  chain.removeInterceptor(writerInterceptor.getClass());
               }
            } else {
               chain.removeInterceptor(writerInterceptor.getClass());
            }
            enabled = false;
         }
      }
   }

   @Override
   public <T> Set<T> getStores(Class<T> storeClass) {
      storesMutex.readLock().lock();
      try {
         Set<T> result = new HashSet<>();
         for (CacheLoader l : loaders) {
            CacheLoader real = undelegate(l);
            if (storeClass.isInstance(real)) {
               result.add(storeClass.cast(real));
            }
         }

         Consumer<CacheWriter> getWriters = writer -> {
            CacheWriter real = undelegate(writer);
            if (storeClass.isInstance(real)) {
               result.add(storeClass.cast(real));
            }
         };
         nonTxWriters.forEach(getWriters);
         txWriters.forEach(getWriters);

         return result;
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   public Collection<String> getStoresAsString() {
      storesMutex.readLock().lock();
      try {
         Set<String> loaderTypes = new HashSet<>(loaders.size());
         for (CacheLoader loader : loaders)
            loaderTypes.add(undelegate(loader).getClass().getName());
         for (CacheWriter writer : nonTxWriters)
            loaderTypes.add(undelegate(writer).getClass().getName());
         for (CacheWriter writer : txWriters)
            loaderTypes.add(undelegate(writer).getClass().getName());
         return loaderTypes;
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   private static class AdvancedPurgeListener<K, V> implements AdvancedCacheExpirationWriter.ExpirationPurgeListener<K, V> {
      private final ExpirationManager<K, V> expirationManager;

      private AdvancedPurgeListener(ExpirationManager<K, V> expirationManager) {
         this.expirationManager = expirationManager;
      }

      @Override
      public void marshalledEntryPurged(MarshalledEntry<K, V> entry) {
         expirationManager.handleInStoreExpiration(entry);
      }

      @Override
      public void entryPurged(K key) {
         expirationManager.handleInStoreExpiration(key);
      }
   }

   @Override
   public void purgeExpired() {
      if (!enabled)
         return;
      long start = -1;
      try {
         if (trace) {
            log.trace("Purging cache store of expired entries");
            start = timeService.time();
         }

         storesMutex.readLock().lock();
         try {
            Consumer<CacheWriter> purgeWriter = writer -> {
               // ISPN-6711 Shared stores should only be purged by the coordinator
               if (configMap.get(writer).shared() && !cache.getCacheManager().isCoordinator())
                  return;

               if (writer instanceof AdvancedCacheExpirationWriter) {
                  //noinspection unchecked
                  ((AdvancedCacheExpirationWriter)writer).purge(persistenceExecutor, advancedListener);
               } else if (writer instanceof AdvancedCacheWriter) {
                  //noinspection unchecked
                  ((AdvancedCacheWriter)writer).purge(persistenceExecutor, advancedListener);
               }
            };
            nonTxWriters.forEach(purgeWriter);
            txWriters.forEach(purgeWriter);
         } finally {
            storesMutex.readLock().unlock();
         }

         if (trace) {
            log.tracef("Purging cache store completed in %s",
                       Util.prettyPrintTime(timeService.timeDuration(start, TimeUnit.MILLISECONDS)));
         }
      } catch (Exception e) {
         log.exceptionPurgingDataContainer(e);
      }
   }

   @Override
   public void clearAllStores(AccessMode mode) {
      storesMutex.readLock().lock();
      try {
         // Apply to txWriters as well as clear does not happen in a Tx context
         Consumer<CacheWriter> clearWriter = writer -> {
            if (writer instanceof AdvancedCacheWriter) {
               if (mode.canPerform(configMap.get(writer))) {
                  ((AdvancedCacheWriter) writer).clear();
               }
            }
         };
         nonTxWriters.forEach(clearWriter);
         txWriters.forEach(clearWriter);
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   public boolean deleteFromAllStores(Object key, AccessMode mode) {
      storesMutex.readLock().lock();
      try {
         boolean removed = false;
         for (CacheWriter w : nonTxWriters) {
            if (mode.canPerform(configMap.get(w))) {
               removed |= w.delete(key);
            }
         }
         return removed;
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   public void processOnAllStores(KeyFilter keyFilter, AdvancedCacheLoader.CacheLoaderTask task,
                                  boolean fetchValue, boolean fetchMetadata) {
      processOnAllStores(persistenceExecutor, keyFilter, task, fetchValue, fetchMetadata);
   }

   @Override
   public void processOnAllStores(Executor executor, KeyFilter keyFilter, AdvancedCacheLoader.CacheLoaderTask task, boolean fetchValue, boolean fetchMetadata) {
      processOnAllStores(executor, keyFilter, task, fetchValue, fetchMetadata, BOTH);
   }

   @Override
   public void processOnAllStores(KeyFilter keyFilter, AdvancedCacheLoader.CacheLoaderTask task,
                                  boolean fetchValue, boolean fetchMetadata, AccessMode mode) {
      processOnAllStores(persistenceExecutor, keyFilter, task, fetchValue, fetchMetadata, mode);
   }

   @Override
   public void processOnAllStores(Executor executor, KeyFilter keyFilter, AdvancedCacheLoader.CacheLoaderTask task, boolean fetchValue, boolean fetchMetadata, AccessMode mode) {
      storesMutex.readLock().lock();
      try {
         for (CacheLoader loader : loaders) {
            if (mode.canPerform(configMap.get(loader)) && loader instanceof AdvancedCacheLoader) {
               //noinspection unchecked
               ((AdvancedCacheLoader) loader).process(keyFilter, task, executor, fetchValue, fetchMetadata);
            }
         }
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   public MarshalledEntry loadFromAllStores(Object key, boolean localInvocation) {
      storesMutex.readLock().lock();
      try {
         for (CacheLoader l : loaders) {
            if (!localInvocation && isLocalOnlyLoader(l))
               continue;

            MarshalledEntry load = l.load(key);
            if (load != null)
               return load;
         }
         return null;
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   private boolean isLocalOnlyLoader(CacheLoader loader) {
      if (loader instanceof LocalOnlyCacheLoader) return true;
      if (loader instanceof DelegatingCacheLoader) {
         CacheLoader unwrappedLoader = ((DelegatingCacheLoader) loader).undelegate();
         return unwrappedLoader instanceof LocalOnlyCacheLoader;
      }
      return false;
   }

   @Override
   public void writeToAllNonTxStores(MarshalledEntry marshalledEntry, AccessMode accessMode) {
      writeToAllNonTxStores(marshalledEntry, accessMode, 0L);
   }

   @Override
   public void writeToAllNonTxStores(MarshalledEntry marshalledEntry, AccessMode accessMode, long flags) {
      storesMutex.readLock().lock();
      try {
         //noinspection unchecked
         nonTxWriters.stream()
               .filter(writer -> !(writer instanceof FlagAffectedStore) || FlagAffectedStore.class.cast(writer).shouldWrite(flags))
               .filter(writer -> accessMode.canPerform(configMap.get(writer)))
               .forEach(writer -> writer.write(marshalledEntry));
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   public void writeBatchToAllNonTxStores(Iterable<MarshalledEntry> entries, AccessMode accessMode, long flags) {
      storesMutex.readLock().lock();
      try {
         //noinspection unchecked
         nonTxWriters.stream()
               .filter(writer -> !(writer instanceof FlagAffectedStore) || FlagAffectedStore.class.cast(writer).shouldWrite(flags))
               .filter(writer -> accessMode.canPerform(configMap.get(writer)))
               .forEach(writer -> writer.writeBatch(entries));
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   public void deleteBatchFromAllNonTxStores(Iterable<Object> keys, AccessMode accessMode, long flags) {
      storesMutex.readLock().lock();
      try {
         nonTxWriters.stream()
               .filter(writer -> accessMode.canPerform(configMap.get(writer)))
               .forEach(writer -> writer.deleteBatch(keys));
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   public void prepareAllTxStores(Transaction transaction, BatchModification batchModification,
                                  AccessMode accessMode) throws PersistenceException {
      storesMutex.readLock().lock();
      try {
         for (CacheWriter writer : txWriters) {
            if (accessMode.canPerform(configMap.get(writer)) || configuration.clustering().cacheMode().equals(CacheMode.LOCAL)) {
               TransactionalCacheWriter txWriter = (TransactionalCacheWriter) undelegate(writer);
               txWriter.prepareWithModifications(transaction, batchModification);
            }
         }
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   public void commitAllTxStores(Transaction transaction, AccessMode accessMode) {
      performOnAllTxStores(accessMode, writer -> writer.commit(transaction));
   }

   @Override
   public void rollbackAllTxStores(Transaction transaction, AccessMode accessMode) {
      performOnAllTxStores(accessMode, writer -> writer.rollback(transaction));
   }

   @Override
   public AdvancedCacheLoader getStateTransferProvider() {
      storesMutex.readLock().lock();
      try {
         for (CacheLoader l : loaders) {
            StoreConfiguration storeConfiguration = configMap.get(l);
            if (storeConfiguration.fetchPersistentState() && !storeConfiguration.shared())
               return (AdvancedCacheLoader) l;
         }
         return null;
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   public int size() {
      storesMutex.readLock().lock();
      try {
         for (CacheLoader l : loaders) {
            if (l instanceof AdvancedCacheLoader)
               return ((AdvancedCacheLoader) l).size();
         }
      } finally {
         storesMutex.readLock().unlock();
      }
      return 0;
   }

   @Override
   public void setClearOnStop(boolean clearOnStop) {
      this.clearOnStop = clearOnStop;
   }

   public List<CacheLoader> getAllLoaders() {
      storesMutex.readLock().lock();
      try {
         return new ArrayList<>(loaders);
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   public List<CacheWriter> getAllWriters() {
      storesMutex.readLock().lock();
      try {
         return new ArrayList<>(nonTxWriters);
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   public List<CacheWriter> getAllTxWriters() {
      storesMutex.readLock().lock();
      try {
         return new ArrayList<>(txWriters);
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   private void createLoadersAndWriters() {
      for (StoreConfiguration cfg : configuration.persistence().stores()) {
         Object bareInstance = cacheStoreFactoryRegistry.createInstance(cfg);

         StoreConfiguration processedConfiguration = cacheStoreFactoryRegistry.processStoreConfiguration(cfg);

         CacheWriter writer = createCacheWriter(bareInstance);
         CacheLoader loader = createCacheLoader(bareInstance);

         writer = postProcessWriter(processedConfiguration, writer);
         loader = postProcessReader(processedConfiguration, writer, loader);

         InitializationContextImpl ctx = new InitializationContextImpl(processedConfiguration, cache, m, timeService, byteBufferFactory,
                                                                       marshalledEntryFactory);
         initializeLoader(processedConfiguration, loader, ctx);
         initializeWriter(processedConfiguration, writer, ctx);
         initializeBareInstance(bareInstance, ctx);
      }
   }

   private CacheLoader postProcessReader(StoreConfiguration cfg, CacheWriter writer, CacheLoader loader) {
      if(cfg.async().enabled() && loader != null && writer != null) {
         loader = createAsyncLoader(loader, (AsyncCacheWriter) writer);
      }
      return loader;
   }

   private CacheWriter postProcessWriter(StoreConfiguration cfg, CacheWriter writer) {
      if (writer != null) {
         if(cfg.ignoreModifications()) {
            writer = null;
         } else if (cfg.singletonStore().enabled()) {
            writer = createSingletonWriter(cfg, writer);
         } else if (cfg.async().enabled()) {
            writer = createAsyncWriter(writer);
         }
      }
      return writer;
   }

   private CacheLoader createAsyncLoader(CacheLoader loader, AsyncCacheWriter asyncWriter) {
      AtomicReference<State> state = asyncWriter.getState();
      loader = (loader instanceof AdvancedCacheLoader) ?
            new AdvancedAsyncCacheLoader(loader, state) : new AsyncCacheLoader(loader, state);
      return loader;
   }

   private SingletonCacheWriter createSingletonWriter(StoreConfiguration cfg, CacheWriter writer) {
      return (writer instanceof AdvancedCacheWriter) ?
            new AdvancedSingletonCacheWriter(writer, cfg.singletonStore()) :
            new SingletonCacheWriter(writer, cfg.singletonStore());
   }

   private void initializeWriter(StoreConfiguration cfg, CacheWriter writer, InitializationContextImpl ctx) {
      if (writer != null) {
         if (writer instanceof DelegatingCacheWriter)
            writer.init(ctx);

         storesMutex.writeLock().lock();
         try {
            if (undelegate(writer) instanceof TransactionalCacheWriter && cfg.transactional()) {
               if (configuration.transaction().transactionMode().isTransactional()) {
                  txWriters.add((TransactionalCacheWriter) writer);
               } else {
                  // If cache is non-transactional then it is not possible for the store to be, so treat as normal store
                  // Shouldn't happen as a CacheConfigurationException should be thrown on validation
                  nonTxWriters.add(writer);
               }
            } else {
               nonTxWriters.add(writer);
            }
         } finally {
            storesMutex.writeLock().unlock();
         }

         configMap.put(writer, cfg);
      }
   }

   private void initializeLoader(StoreConfiguration cfg, CacheLoader loader, InitializationContextImpl ctx) {
      if (loader != null) {
         if (loader instanceof DelegatingCacheLoader)
            loader.init(ctx);
         storesMutex.writeLock().lock();
         try {
            loaders.add(loader);
         } finally {
            storesMutex.writeLock().unlock();
         }
         configMap.put(loader, cfg);
      }
   }

   private void initializeBareInstance(Object instance, InitializationContextImpl ctx) {
      // the delegates only propagate init if the underlaying object is a delegate as well.
      // we do this in order to assure the init is only invoked once
      if (instance instanceof CacheWriter) {
         ((CacheWriter) instance).init(ctx);
      } else {
         ((CacheLoader) instance).init(ctx);
      }
   }

   private CacheLoader createCacheLoader(Object instance) {
      return instance instanceof CacheLoader ? (CacheLoader) instance : null;
   }

   private CacheWriter createCacheWriter(Object instance) {
      return instance instanceof CacheWriter ? (CacheWriter) instance : null;
   }

   protected AsyncCacheWriter createAsyncWriter(CacheWriter writer) {
      return (writer instanceof AdvancedCacheWriter) ?
            new AdvancedAsyncCacheWriter(writer) : new AsyncCacheWriter(writer);
   }

   private CacheLoader undelegate(CacheLoader l) {
      return (l instanceof DelegatingCacheLoader) ? ((DelegatingCacheLoader)l).undelegate() : l;
   }

   private CacheWriter undelegate(CacheWriter w) {
      return (w instanceof DelegatingCacheWriter) ? ((DelegatingCacheWriter)w).undelegate() : w;

   }

   private AdvancedCache<Object, Object> getCacheForStateInsertion() {
      List<Flag> flags = new ArrayList<>(Arrays.asList(
            CACHE_MODE_LOCAL, SKIP_OWNERSHIP_CHECK, IGNORE_RETURN_VALUES, SKIP_CACHE_STORE, SKIP_LOCKING,
            SKIP_XSITE_BACKUP));

      boolean hasShared = false;
      storesMutex.readLock().lock();
      try {
         for (CacheWriter w : nonTxWriters) {
            if (configMap.get(w).shared()) {
               hasShared = true;
               break;
            }
         }
      } finally {
         storesMutex.readLock().unlock();
      }

      if (hasShared) {
         if (indexShareable())
            flags.add(SKIP_INDEXING);
      } else {
         flags.add(SKIP_INDEXING);
      }

      return cache.getAdvancedCache()
            .withFlags(flags.toArray(new Flag[flags.size()]));
   }

   private boolean indexShareable() {
      return configuration.indexing().indexShareable();
   }

   private long getMaxEntries() {
      if (configuration.memory().isEvictionEnabled()&& configuration.memory().evictionType() == EvictionType.COUNT)
         return configuration.memory().size();
      return Long.MAX_VALUE;
   }

   private void preloadKey(AdvancedCache<Object, Object> cache, Object key, Object value, Metadata metadata) {
      final Transaction transaction = suspendIfNeeded();
      boolean success = false;
      try {
         try {
            beginIfNeeded();
            cache.put(key, value, metadata);
            success = true;
         } catch (Exception e) {
            throw new PersistenceException("Unable to preload!", e);
         } finally {
            commitIfNeeded(success);
         }
      } finally {
         //commitIfNeeded can throw an exception, so we need a try { } finally { }
         resumeIfNeeded(transaction);
      }
   }

   private void resumeIfNeeded(Transaction transaction) {
      if (configuration.transaction().transactionMode().isTransactional() && transactionManager != null &&
            transaction != null) {
         try {
            transactionManager.resume(transaction);
         } catch (Exception e) {
            throw new PersistenceException(e);
         }
      }
   }

   private Transaction suspendIfNeeded() {
      if (configuration.transaction().transactionMode().isTransactional() && transactionManager != null) {
         try {
            return transactionManager.suspend();
         } catch (Exception e) {
            throw new PersistenceException(e);
         }
      }
      return null;
   }

   private void beginIfNeeded() {
      if (configuration.transaction().transactionMode().isTransactional() && transactionManager != null) {
         try {
            transactionManager.begin();
         } catch (Exception e) {
            throw new PersistenceException(e);
         }
      }
   }

   private void commitIfNeeded(boolean success) {
      if (configuration.transaction().transactionMode().isTransactional() && transactionManager != null) {
         try {
            if (success) {
               transactionManager.commit();
            } else {
               transactionManager.rollback();
            }
         } catch (Exception e) {
            throw new PersistenceException(e);
         }
      }
   }

   public StreamingMarshaller getMarshaller() {
      return m;
   }

   private void removeCacheLoader(String storeType, Collection<CacheLoader> collection) {
      for (Iterator<CacheLoader> it = collection.iterator(); it.hasNext(); ) {
         CacheLoader loader = it.next();
         doRemove(it, storeType, loader, undelegate(loader));
      }
   }

   private void removeCacheWriter(String storeType, Collection<? extends CacheWriter> collection) {
      for (Iterator<? extends CacheWriter> it = collection.iterator(); it.hasNext(); ) {
         CacheWriter writer = it.next();
         doRemove(it, storeType, writer, undelegate(writer));
      }
   }

   private void doRemove(Iterator<? extends Lifecycle> it, String storeType, Lifecycle wrapper, Lifecycle actual) {
      if (actual.getClass().getName().equals(storeType)) {
         wrapper.stop();
         if (actual != wrapper) {
            actual.stop();
         }
         it.remove();
      }
   }

   private void performOnAllTxStores(AccessMode accessMode, Consumer<TransactionalCacheWriter> action) {
      storesMutex.readLock().lock();
      try {
         txWriters.stream()
               .filter(writer -> accessMode.canPerform(configMap.get(writer)))
               .forEach(action);
      } finally {
         storesMutex.readLock().unlock();
      }
   }
}
