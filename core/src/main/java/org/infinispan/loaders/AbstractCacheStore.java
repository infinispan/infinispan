package org.infinispan.loaders;

import org.infinispan.Cache;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.loaders.modifications.Remove;
import org.infinispan.loaders.modifications.Store;
import org.infinispan.marshall.Marshaller;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An abstract {@link org.infinispan.loaders.CacheStore} that holds common implementations for some methods
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class AbstractCacheStore extends AbstractCacheLoader implements CacheStore {

   private Map<GlobalTransaction, List<? extends Modification>> transactions;
   private static Log log = LogFactory.getLog(AbstractCacheStore.class);
   private AbstractCacheStoreConfig config;
   protected ExecutorService purgerService;
   protected Marshaller marshaller;
   protected Cache cache;
   private static final AtomicInteger THREAD_COUNTER = new AtomicInteger(0);
   protected boolean multiThreadedPurge = false;

   public void init(CacheLoaderConfig config, Cache<?, ?> cache, Marshaller m) throws CacheLoaderException{
      this.config = (AbstractCacheStoreConfig) config;
      this.marshaller = m;
      if (config == null) throw new IllegalStateException("Null config!!!");
      this.cache = cache;
   }

   protected final int getConcurrencyLevel() {
      return cache == null || cache.getConfiguration() == null? 16 : cache.getConfiguration().getConcurrencyLevel();
   }

   public void start() throws CacheLoaderException {
      if (config == null) throw new IllegalStateException("Make sure you call super.init() from CacheStore extension");
      if (config.isPurgeSynchronously()) {
         purgerService = new WithinThreadExecutor();
      } else {
         multiThreadedPurge = supportsMultiThreadedPurge() && config.getPurgerThreads() > 1;
         final String loaderName = getClass().getSimpleName();
         purgerService = Executors.newFixedThreadPool(supportsMultiThreadedPurge() ? config.getPurgerThreads() : 1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
               // Thread name: <cache>-<CacheStore>-<purger>-ID
               Thread t = new Thread(r, (cache == null ? "" : cache.getName() + "-") + loaderName + "-" + THREAD_COUNTER.getAndIncrement());
               t.setDaemon(true);
               return t;
            }
         });
      }
      transactions = new ConcurrentHashMap<GlobalTransaction, List<? extends Modification>>(64, 0.75f, getConcurrencyLevel());
   }

   protected boolean supportsMultiThreadedPurge() {
      return false;
   }

   public void stop() throws CacheLoaderException {
      purgerService.shutdownNow();
   }

   public void purgeExpired() throws CacheLoaderException {
      if (purgerService == null)
         throw new IllegalStateException("purgerService is null (did you call super.start() from cache loader implementation ?");
      purgerService.execute(new Runnable() {
         public void run() {
            try {
               purgeInternal();
            } catch (CacheLoaderException e) {
               log.error("Problems encountered while purging expired", e);
            }
         }
      });
   }

   protected abstract void purgeInternal() throws CacheLoaderException;

   protected void applyModifications(List<? extends Modification> mods) throws CacheLoaderException {
      for (Modification m : mods) {
         switch (m.getType()) {
            case STORE:
               Store s = (Store) m;
               store(s.getStoredEntry());
               break;
            case CLEAR:
               clear();
               break;
            case REMOVE:
               Remove r = (Remove) m;
               remove(r.getKey());
               break;
            default:
               throw new IllegalArgumentException("Unknown modification type " + m.getType());
         }
      }
   }

   public void prepare(List<? extends Modification> mods, GlobalTransaction tx, boolean isOnePhase) throws CacheLoaderException {
      if (isOnePhase) {
         applyModifications(mods);
      } else {
         transactions.put(tx, mods);
      }
   }

   public void rollback(GlobalTransaction tx) {
      transactions.remove(tx);
   }

   public CacheStoreConfig getCacheStoreConfig() {
      return config;
   }

   public void commit(GlobalTransaction tx) throws CacheLoaderException {
      List<? extends Modification> list = transactions.remove(tx);
      if (list != null && !list.isEmpty()) applyModifications(list);
   }

   public void removeAll(Set<Object> keys) throws CacheLoaderException {
      if (keys != null && !keys.isEmpty()) {
         for (Object key : keys) remove(key);
      }
   }

   protected final void safeClose(InputStream stream) throws CacheLoaderException {
      if (stream == null) return;
      try {
         stream.close();
      } catch (Exception e) {
         throw new CacheLoaderException("Problems closing input stream", e);
      }
   }

   protected final void safeClose(OutputStream stream) throws CacheLoaderException {
      if (stream == null) return;
      try {
         stream.close();
      } catch (Exception e) {
         throw new CacheLoaderException("Problems closing output stream", e);
      }
   }

   protected Marshaller getMarshaller() {
      return marshaller;
   }
}
