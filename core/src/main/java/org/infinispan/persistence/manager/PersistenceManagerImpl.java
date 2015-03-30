package org.infinispan.persistence.manager;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.io.ByteBufferFactory;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.filter.KeyFilter;
import org.infinispan.interceptors.CacheLoaderInterceptor;
import org.infinispan.interceptors.CacheWriterInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.persistence.InitializationContextImpl;
import org.infinispan.persistence.async.AdvancedAsyncCacheLoader;
import org.infinispan.persistence.async.AdvancedAsyncCacheWriter;
import org.infinispan.persistence.async.AsyncCacheLoader;
import org.infinispan.persistence.async.AsyncCacheWriter;
import org.infinispan.persistence.async.State;
import org.infinispan.persistence.factory.CacheStoreFactoryRegistry;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.persistence.spi.AdvancedCacheWriter;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.CacheWriter;
import org.infinispan.persistence.spi.LocalOnlyCacheLoader;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.support.AdvancedSingletonCacheWriter;
import org.infinispan.persistence.support.DelegatingCacheLoader;
import org.infinispan.persistence.support.DelegatingCacheWriter;
import org.infinispan.persistence.support.SingletonCacheWriter;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.context.Flag.*;
import static org.infinispan.factories.KnownComponentNames.CACHE_MARSHALLER;
import static org.infinispan.factories.KnownComponentNames.PERSISTENCE_EXECUTOR;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;

public class PersistenceManagerImpl implements PersistenceManager {

   private static final Log log = LogFactory.getLog(PersistenceManagerImpl.class);

   Configuration configuration;
   AdvancedCache<Object, Object> cache;
   StreamingMarshaller m;

   TransactionManager transactionManager;
   private TimeService timeService;
   private final List<CacheLoader> loaders = new ArrayList<>();
   private final List<CacheWriter> writers = new ArrayList<>();

   private final ReadWriteLock storesMutex = new ReentrantReadWriteLock();
   private final Map<Object, StoreConfiguration> configMap = new HashMap<>();

   private CacheStoreFactoryRegistry cacheStoreFactoryRegistry;

   /**
    * making it volatile as it might change after @Start, so it needs the visibility.
    */
   volatile boolean enabled;
   private Executor persistenceExecutor;
   private ByteBufferFactory byteBufferFactory;
   private MarshalledEntryFactory marshalledEntryFactory;
   private volatile boolean clearOnStop;

   @Inject
   public void inject(AdvancedCache<Object, Object> cache, @ComponentName(CACHE_MARSHALLER) StreamingMarshaller marshaller,
                      Configuration configuration, TransactionManager transactionManager,
                      TimeService timeService, @ComponentName(PERSISTENCE_EXECUTOR) ExecutorService persistenceExecutor,
                      ByteBufferFactory byteBufferFactory, MarshalledEntryFactory marshalledEntryFactory, CacheStoreFactoryRegistry cacheStoreFactoryRegistry) {
      this.cache = cache;
      this.m = marshaller;
      this.configuration = configuration;
      this.transactionManager = transactionManager;
      this.timeService = timeService;
      this.persistenceExecutor = persistenceExecutor;
      this.byteBufferFactory = byteBufferFactory;
      this.marshalledEntryFactory = marshalledEntryFactory;
      this.cacheStoreFactoryRegistry = cacheStoreFactoryRegistry;
   }

   @Override
   @Start(priority = 10)
   public void start() {
      enabled = configuration.persistence().usingStores();
      if (!enabled)
         return;
      try {
         createLoadersAndWriters();
         Transaction xaTx = null;
         if (transactionManager != null) {
            xaTx = transactionManager.suspend();
         }
         try {

            Set undelegated = new HashSet();//black magic to make sure the store start only gets invoked once
            for (CacheWriter w : writers) {
               w.start();
               if (w instanceof DelegatingCacheWriter) {
                  CacheWriter actual = undelegate(w);
                  actual.start();
                  undelegated.add(actual);
               } else {
                  undelegated.add(w);
               }

               if (configMap.get(w).purgeOnStartup()) {
                  if (!(w instanceof AdvancedCacheWriter))
                     throw new PersistenceException("'purgeOnStartup' can only be set on stores implementing " +
                                                          "" + AdvancedCacheWriter.class.getName());
                  ((AdvancedCacheWriter) w).clear();
               }
            }

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
         }
      } catch (Exception e) {
         throw new CacheException("Unable to start cache loaders", e);
      }
   }

   @Override
   @Stop
   public void stop() {
      // If needed, clear the persistent store before stopping
      if (clearOnStop)
         clearAllStores(AccessMode.BOTH);

      Set undelegated = new HashSet();
      for (CacheWriter w : writers) {
         w.stop();
         if (w instanceof DelegatingCacheWriter) {
            CacheWriter actual = undelegate(w);
            actual.stop();
            undelegated.add(actual);
         } else {
            undelegated.add(w);
         }
      }

      for (CacheLoader l : loaders) {
         if (!undelegated.contains(l))
            l.stop();
         if (l instanceof DelegatingCacheLoader) {
            CacheLoader actual = undelegate(l);
            if (!undelegated.contains(actual)) {
               actual.stop();
            }
         }
      }

   }

   @Override
   @Start(priority = 56)
   public void preload() {
      if (!enabled)
         return;
      AdvancedCacheLoader preloadCl = null;

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
      if (preloadCl == null)
         return;

      long start = timeService.time();


      final int maxEntries = getMaxEntries();
      final AtomicInteger loadedEntries = new AtomicInteger(0);
      final AdvancedCache<Object, Object> flaggedCache = getCacheForStateInsertion();
      preloadCl.process(null, new AdvancedCacheLoader.CacheLoaderTask() {
         @Override
         public void processEntry(MarshalledEntry me, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
            if (loadedEntries.getAndIncrement() >= maxEntries) {
               taskContext.stop();
               return;
            }
            Metadata metadata = me.getMetadata() != null ? ((InternalMetadataImpl)me.getMetadata()).actual() : null; //the downcast will go away with ISPN-3460
            preloadKey(flaggedCache, me.getKey(), me.getValue(), metadata);
         }
      }, new WithinThreadExecutor(), true, true);

      log.debugf("Preloaded %s keys in %s", loadedEntries, Util.prettyPrintTime(timeService.timeDuration(start, MILLISECONDS)));
   }

   @Override
   public void disableStore(String storeType) {
      if (enabled) {
         storesMutex.writeLock().lock();
         try {
            Iterator<CacheLoader> clIt = loaders.iterator();
            while (clIt.hasNext()) {
               CacheLoader l = clIt.next();
               if (undelegate(l).getClass().getName().equals(storeType))
                  clIt.remove();
            }
            Iterator<CacheWriter> cwIt = writers.iterator();
            while (cwIt.hasNext()) {
               CacheWriter w = cwIt.next();
               if (undelegate(w).getClass().getName().equals(storeType))
                  cwIt.remove();
            }
         } finally {
            storesMutex.writeLock().unlock();
         }

         if (loaders.isEmpty() && writers.isEmpty()) {
            InterceptorChain chain = cache.getComponentRegistry().getComponent(InterceptorChain.class);
            List<CommandInterceptor> loaderInterceptors = chain.getInterceptorsWhichExtend(CacheLoaderInterceptor.class);
            if (loaderInterceptors.isEmpty()) {
               log.persistenceWithoutCacheLoaderInterceptor();
            } else {
               for (CommandInterceptor interceptor : loaderInterceptors) {
                  ((CacheLoaderInterceptor) interceptor).disableInterceptor();
               }
            }
            List<CommandInterceptor> writerInterceptors = chain.getInterceptorsWhichExtend(CacheWriterInterceptor.class);
            if (writerInterceptors.isEmpty()) {
               log.persistenceWithoutCacheWriteInterceptor();
            } else {
               for (CommandInterceptor interceptor : writerInterceptors) {
                  ((CacheWriterInterceptor) interceptor).disableInterceptor();
               }
            }

            removeInterceptors(loaderInterceptors);
            removeInterceptors(writerInterceptors);
            enabled = false;
         }
      }
   }

   @Override
   public <T> Set<T> getStores(Class<T> storeClass) {
      storesMutex.readLock().lock();
      try {
         Set<T> result = new HashSet<T>();
         for (CacheLoader l : loaders) {
            CacheLoader real = undelegate(l);
            if (storeClass.isInstance(real)) {
               result.add((T) real);
            }
         }
         for (CacheWriter w : writers) {
            CacheWriter real = undelegate(w);
            if (storeClass.isInstance(real))
               result.add((T) real);
         }
         return result;
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   public Collection<String> getStoresAsString() {
      storesMutex.readLock().lock();
      try {
         Set<String> loaderTypes = new HashSet<String>(loaders.size());
         for (CacheLoader loader : loaders)
            loaderTypes.add(undelegate(loader).getClass().getName());
         for (CacheWriter writer : writers)
            loaderTypes.add(undelegate(writer).getClass().getName());
         return loaderTypes;
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   public void purgeExpired() {
      if (!enabled)
         return;

      long start = -1;
      try {
         if (log.isTraceEnabled()) {
            log.trace("Purging cache store of expired entries");
            start = timeService.time();
         }

         storesMutex.readLock().lock();
         try {
            for (CacheWriter w : writers) {
               if (w instanceof AdvancedCacheWriter) {
                  final CacheNotifier notifier = cache.getComponentRegistry().getComponent(CacheNotifier.class);
                  ((AdvancedCacheWriter)w).purge(persistenceExecutor, new AdvancedCacheWriter.PurgeListener() {
                     @Override
                     public void entryPurged(Object key) {
                        InternalCacheEntry ice = new ImmortalCacheEntry(key, null);
                        notifier.notifyCacheEntriesEvicted(Collections.singleton(ice), null, null);
                     }
                  });
               }
            }
         } finally {
            storesMutex.readLock().unlock();
         }

         if (log.isTraceEnabled()) {
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
         for (CacheWriter w : writers) {
            if (w instanceof AdvancedCacheWriter) {
               if (mode.canPerform(configMap.get(w))) {
                  ((AdvancedCacheWriter) w).clear();
               }
            }
         }
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   public boolean deleteFromAllStores(Object key, AccessMode mode) {
      storesMutex.readLock().lock();
      try {
         boolean removed = false;
         for (CacheWriter w : writers) {
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
               ((AdvancedCacheLoader) loader).process(keyFilter, task, executor, fetchValue, fetchMetadata);
            }
         }
      } finally {
         storesMutex.readLock().unlock();
      }
   }

   @Override
   public MarshalledEntry loadFromAllStores(Object key, InvocationContext context) {
      storesMutex.readLock().lock();
      try {
         for (CacheLoader l : loaders) {
            if (!context.isOriginLocal() && isLocalOnlyLoader(l))
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
         if (unwrappedLoader instanceof LocalOnlyCacheLoader)
            return true;
      }
      return false;
   }

   @Override
   public void writeToAllStores(MarshalledEntry marshalledEntry, AccessMode mode) {
      storesMutex.readLock().lock();
      try {
         for (CacheWriter w : writers) {
            if (mode.canPerform(configMap.get(w))) {
               w.write(marshalledEntry);
            }
         }
      } finally {
         storesMutex.readLock().unlock();
      }
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
      return Collections.unmodifiableList(loaders);
   }

   public List<CacheWriter> getAllWriters() {
      return Collections.unmodifiableList(writers);
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
         writers.add(writer);
         configMap.put(writer, cfg);
      }
   }

   private void initializeLoader(StoreConfiguration cfg, CacheLoader loader, InitializationContextImpl ctx) {
      if (loader != null) {
         if (loader instanceof DelegatingCacheLoader)
            loader.init(ctx);
         loaders.add(loader);
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
      List<Flag> flags = new ArrayList<Flag>(Arrays.asList(
            CACHE_MODE_LOCAL, SKIP_OWNERSHIP_CHECK, IGNORE_RETURN_VALUES, SKIP_CACHE_STORE, SKIP_LOCKING));

      boolean hasShared = false;
      for (CacheWriter w : writers) {
         if (configMap.get(w).shared()) {
            hasShared = true;
            break;
         }
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

   private boolean localIndexingEnabled() {
      return configuration.indexing().index() == Index.LOCAL;
   }

   private boolean indexShareable() {
      return configuration.indexing().indexShareable();
   }

   private int getMaxEntries() {
      int ne = Integer.MAX_VALUE;
      if (configuration.eviction().strategy().isEnabled()) ne = configuration.eviction().maxEntries();
      return ne;
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

   public Executor getPersistenceExecutor() {
      return persistenceExecutor;
   }

   public StreamingMarshaller getMarshaller() {
      return m;
   }

   private void removeInterceptors(Collection<CommandInterceptor> interceptors) {
      if (interceptors.isEmpty()) {
         return;
      }
      for (CommandInterceptor interceptor : interceptors) {
         cache.removeInterceptor(interceptor.getClass());
      }
   }
}
